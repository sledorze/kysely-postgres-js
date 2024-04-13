import {CompiledQuery, DatabaseConnection, QueryResult, TransactionSettings} from 'kysely'
// import type {ReservedSql} from 'postgres'
import {PGlite} from '@electric-sql/pglite'
import {PostgresJSDialectError} from './errors.js'

export class PostgresJSConnection implements DatabaseConnection {
  #reservedConnection: PGlite

  constructor(reservedConnection: PGlite) {
    this.#reservedConnection = reservedConnection
  }

  async beginTransaction(settings: TransactionSettings): Promise<void> {
    const {isolationLevel} = settings
    await this.#reservedConnection.exec(
      isolationLevel ? `start transaction isolation level ${isolationLevel}` : 'begin',
    )
  }

  async commitTransaction(): Promise<void> {
    await this.#reservedConnection.exec('commit')
  }

  async executeQuery<R>(compiledQuery: CompiledQuery<unknown>): Promise<QueryResult<R>> {
    const result = await this.#reservedConnection.query(compiledQuery.sql, compiledQuery.parameters.slice() as any)

    const rows = Array.from<R>(result.rows as R[])

    // if (['INSERT', 'UPDATE', 'DELETE'].includes(result.command)) {
    //   const numAffectedRows = BigInt(result.count)
    //   return {numAffectedRows, rows}
    // }

    return {rows, numAffectedRows: result.affectedRows ? BigInt(result.affectedRows) : undefined}
  }

  releaseConnection(): void {
    this.#reservedConnection = null!
  }

  async rollbackTransaction(): Promise<void> {
    await this.#reservedConnection.exec('rollback')
  }

  async *streamQuery<R>(
    compiledQuery: CompiledQuery<unknown>,
    chunkSize: number,
  ): AsyncIterableIterator<QueryResult<R>> {
    throw new Error('no cursor support ATM')
    if (!Number.isInteger(chunkSize) || chunkSize <= 0) {
      throw new PostgresJSDialectError('chunkSize must be a positive integer')
    }

    const cursor = this.#reservedConnection
      .query(compiledQuery.sql, compiledQuery.parameters.slice() as any)
      .cursor(chunkSize)

    for await (const rows of cursor) {
      yield {rows}
    }
  }
}
