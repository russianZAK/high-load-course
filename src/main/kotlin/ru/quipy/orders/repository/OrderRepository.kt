package ru.quipy.orders.repository

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import ru.quipy.apigateway.APIController
import ru.quipy.apigateway.APIController.Order
import java.util.*
import javax.annotation.PostConstruct


@Repository
class OrderRepository {

    val logger: Logger = LoggerFactory.getLogger(OrderRepository::class.java)

    @Autowired
    lateinit var namedJdbcTemplate: NamedParameterJdbcTemplate

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    private val createTableSql = """
        CREATE TABLE IF NOT EXISTS orders (
            id UUID PRIMARY KEY,
            user_id UUID NOT NULL,
            time_created BIGINT NOT NULL,
            status VARCHAR(255) NOT NULL,
            price INT NOT NULL
        );
    """

    @PostConstruct
    fun init() {
        println("AAAAAAAAAAAAAAAAAAAAAAAAA")
        jdbcTemplate.execute(createTableSql)
    }


    private val findOrderQuery = "SELECT * FROM orders WHERE id = :id"
    private val insertOrderQuery = """
        INSERT INTO orders (id, user_id, time_created, status, price) 
        VALUES (:id, :userId, :timeCreated, :status, :price)
    """
    private val updateOrderQuery = """
        UPDATE orders 
        SET status = :status 
        WHERE id = :id
    """

    fun save(order: Order): Order {
        val params = mapOf(
            "id" to order.id,
            "userId" to order.userId,
            "timeCreated" to order.timeCreated,
            "status" to order.status.name,
            "price" to order.price
        )

        val existingOrders = namedJdbcTemplate.query(findOrderQuery, params) { rs, _ ->
            Order(
                rs.getObject("id", UUID::class.java),
                rs.getObject("user_id", UUID::class.java),
                rs.getLong("time_created"),
                APIController.OrderStatus.valueOf(rs.getString("status")),
                rs.getInt("price")
            )
        }

        return if (existingOrders.isEmpty()) {
            namedJdbcTemplate.update(insertOrderQuery, params)
            order
        } else {
            val existingOrder = existingOrders.first()
            namedJdbcTemplate.update(updateOrderQuery, mapOf("id" to order.id, "status" to order.status.name))
            existingOrder.copy(status = order.status)
        }
    }


    fun findById(id: UUID): Order? {
        val params = mapOf("id" to id)
        return try {
            namedJdbcTemplate.queryForObject(findOrderQuery, params
            ) { rs, _ ->
                Order(
                    rs.getObject("id", UUID::class.java),
                    rs.getObject("user_id", UUID::class.java),
                    rs.getLong("time_created"),
                    APIController.OrderStatus.valueOf(rs.getString("status")),
                    rs.getInt("price")
                )
            }
        } catch (e: EmptyResultDataAccessException) {
            null
        }
    }
}

