import { GRAPHIFY_ROWS_FAILED_ERR } from './RuntimeError'
import { identity } from '../utils'
import { RDBType } from './DataType'

const nestJS = require('nesthydrationjs')()

const LiteralArray = 'LiteralArray'

nestJS.registerType(LiteralArray, identity)

// primaryKey based, key: { id: true } must be given in definition.
export default function<T>(rows: any[], definition: Object) {
  try {
    const result = nestJS.nest(rows, [definition])
    return result as T[]
  } catch (e) {
    throw GRAPHIFY_ROWS_FAILED_ERR(e)
  }
}

/**
 * NestHydrationJS:
 *
 * Nesting is achieved by using a underscore (_).
 * A x to one relation is defined by a single underscore and
 * a x to many relation is defined by preceeding properties of
 * the many object with a 2nd underscore.
 */
export function nestFieldName(name: string, subname: string): string {
  return `${name}__${subname}`
}

/**
 * Specify a part of the definition object that is
 * to be fed to nestJS.nest function.
 */
export function definition(
  fieldName: string,
  asId: boolean,
  type: any
) {
  const matcher = {
    column: fieldName,
    id: asId
  }

  if (type === RDBType.LITERAL_ARRAY) {
     return { ...matcher, type: LiteralArray }
  }

  return matcher
}
