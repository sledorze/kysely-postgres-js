// import type {Sql} from 'postgres'
import type {PGlite} from '@electric-sql/pglite'

export interface PostgresJSDialectConfig {
  readonly postgres: PGlite
}
