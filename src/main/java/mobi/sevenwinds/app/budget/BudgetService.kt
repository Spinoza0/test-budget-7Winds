package mobi.sevenwinds.app.budget

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import mobi.sevenwinds.app.author.AuthorTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.transaction

object BudgetService {
    suspend fun addRecord(body: BudgetRecord): BudgetRecord = withContext(Dispatchers.IO) {
        transaction {
            val entity = BudgetEntity.new {
                this.year = body.year
                this.month = body.month
                this.amount = body.amount
                this.type = body.type
                this.author = body.author
            }

            return@transaction entity.toResponse()
        }
    }

    suspend fun getYearStats(param: BudgetYearParam): BudgetYearStatsResponse = withContext(Dispatchers.IO) {
        transaction {
            val expression = when (param.author) {
                null, "" -> (BudgetTable.year eq param.year)
                else -> (BudgetTable.year eq param.year) and
                        (AuthorTable.fullName.lowerCase() eq param.author.toLowerCase())
            }

            val query = BudgetTable.join(AuthorTable, JoinType.LEFT, null) {
                BudgetTable.author eq AuthorTable.id
            }
                .select { expression }
                .orderBy(BudgetTable.month to SortOrder.ASC, BudgetTable.amount to SortOrder.DESC)

            val total = query.count()
            
            val data = query
                .limit(param.limit, param.offset)
                .map {
                    BudgetResponse(
                        it[BudgetTable.year],
                        it[BudgetTable.month],
                        it[BudgetTable.amount],
                        it[BudgetTable.type],
                        it[AuthorTable.fullName]
                    )
                }

            val sumByType = query.groupBy { it[BudgetTable.type].name }
                .mapValues { it.value.sumOf { v -> v[BudgetTable.amount] } }

            return@transaction BudgetYearStatsResponse(
                total = total,
                totalByType = sumByType,
                items = data
            )
        }
    }
}