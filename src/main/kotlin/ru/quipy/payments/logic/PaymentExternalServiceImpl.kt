package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val tokenBucketRateLimiter = TokenBucketRateLimiter(
        rateLimitPerSec,
        rateLimitPerSec,
        Duration.ofSeconds(1).toMillis(),
        TimeUnit.MILLISECONDS
    )
    private val processingTimeMillis = 5000L
    private val semaphore = FairSemaphore(parallelRequests)
    private val client = OkHttpClient.Builder().callTimeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() + 100)).build()
    private val coroutineScope = CoroutineScope(Dispatchers.IO)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        coroutineScope.launch {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()


            try {
                withTimeout(processingTimeMillis) {
                    retryWithBackOff {
                        processRequest(request, paymentId, transactionId)
                    }
                }
            } catch (e: Exception) {
                handlePaymentException(e, paymentId, transactionId)
            }
        }
    }

    private fun handlePaymentException(e: Exception, paymentId: UUID, transactionId: UUID) {
        when (e) {
            is SocketTimeoutException -> {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            }

            else -> {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    private suspend fun processRequest(request: Request, paymentId: UUID, transactionId: UUID): Boolean {
        val semaphoreStartTime = now()
        return semaphore.withPermit {
            val semaphoreTime = now() - semaphoreStartTime
            val rateLimiterLeftTime = processingTimeMillis - semaphoreTime

            if (!tokenBucketRateLimiter.tryTick(rateLimiterLeftTime, TimeUnit.MILLISECONDS)) {
                throw Exception("Not enough time left for payment processing")
            }
            try {
                val response = client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
                return@withPermit response.success
            } catch (exception: Exception) {
                return@withPermit false
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private suspend fun retryWithBackOff(
        maxAttempts: Int = 4,
        initialDelay: Long = 200,
        block: suspend () -> Boolean
    ) {
        repeat(maxAttempts) {
            val result = block()
            if (result) return
            delay(initialDelay)
        }
    }

}

public fun now() = System.currentTimeMillis()

class FairSemaphore(permits: Int) {
    private val queue = Channel<Unit>(permits)

    init {
        repeat(permits) { queue.trySend(Unit) }
    }

    suspend fun <T> withPermit(action: suspend () -> T): T {
        queue.receive()
        try {
            return action()
        } finally {
            queue.send(Unit)
        }
    }
}