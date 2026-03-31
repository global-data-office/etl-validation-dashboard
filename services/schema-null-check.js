/**
 * Schema-aware null check — pure logic functions.
 *
 * These functions are free of Express / BigQuery dependencies so they
 * can be unit-tested and property-tested in isolation.
 */

const TABLE_FORMAT_RE = /^[\w-]+\.[\w-]+\.[\w-]+$/;

/**
 * Compare source and target schema column arrays.
 *
 * @param {Array<{column_name:string, data_type:string, is_nullable:string}>} sourceColumns
 * @param {Array<{column_name:string, data_type:string, is_nullable:string}>} targetColumns
 * @returns {{ columns: Array, summary: Object }}
 */
function compareSchemas(sourceColumns, targetColumns) {
  const sourceMap = new Map();
  for (const col of sourceColumns) {
    sourceMap.set(col.column_name, col);
  }

  const targetMap = new Map();
  for (const col of targetColumns) {
    targetMap.set(col.column_name, col);
  }

  const columns = [];

  // Process all source columns first
  for (const col of sourceColumns) {
    const target = targetMap.get(col.column_name);
    if (target) {
      columns.push({
        columnName: col.column_name,
        sourceDataType: col.data_type,
        targetDataType: target.data_type,
        dataTypeMatch: col.data_type === target.data_type,
        sourceNullable: col.is_nullable,
        targetNullable: target.is_nullable,
        nullableMatch: col.is_nullable === target.is_nullable,
        status: 'both',
      });
    } else {
      columns.push({
        columnName: col.column_name,
        sourceDataType: col.data_type,
        targetDataType: null,
        dataTypeMatch: false,
        sourceNullable: col.is_nullable,
        targetNullable: null,
        nullableMatch: false,
        status: 'source_only',
      });
    }
  }

  // Process target-only columns
  for (const col of targetColumns) {
    if (!sourceMap.has(col.column_name)) {
      columns.push({
        columnName: col.column_name,
        sourceDataType: null,
        targetDataType: col.data_type,
        dataTypeMatch: false,
        sourceNullable: null,
        targetNullable: col.is_nullable,
        nullableMatch: false,
        status: 'target_only',
      });
    }
  }

  // Build summary
  const bothColumns = columns.filter((c) => c.status === 'both');
  const summary = {
    totalColumnsCompared: columns.length,
    dataTypeMismatches: bothColumns.filter((c) => !c.dataTypeMatch).length,
    nullabilityMismatches: bothColumns.filter((c) => !c.nullableMatch).length,
    sourceOnlyColumns: columns.filter((c) => c.status === 'source_only').length,
    targetOnlyColumns: columns.filter((c) => c.status === 'target_only').length,
  };

  return { columns, summary };
}

/**
 * Identify NOT NULL columns from the source schema, plus the primary key.
 *
 * @param {Array<{column_name:string, is_nullable:string}>} sourceColumns
 * @param {string} primaryKey
 * @returns {string[]} deduplicated column names
 */
function identifyNotNullColumns(sourceColumns, primaryKey) {
  const set = new Set();
  for (const col of sourceColumns) {
    if (col.is_nullable === 'NO') {
      set.add(col.column_name);
    }
  }
  // Always include the primary key
  set.add(primaryKey);
  return Array.from(set);
}

/**
 * Build null-validation results from a COUNTIF query row.
 *
 * @param {string[]} notNullColumns
 * @param {Object}   nullCountRow  — keys like `null_<colname>` with numeric values
 * @param {number}   totalRows
 * @returns {{ results: Array, summary: Object }}
 */
function buildNullValidationResults(notNullColumns, nullCountRow, totalRows) {
  const results = notNullColumns.map((col) => {
    const nullCount = nullCountRow[`null_${col}`] || 0;
    const nonNullCount = totalRows - nullCount;
    const nullPercentage =
      totalRows === 0 ? '0.00' : ((nullCount / totalRows) * 100).toFixed(2);
    return {
      columnName: col,
      totalRows,
      nullCount,
      nonNullCount,
      nullPercentage,
      status: nullCount === 0 ? 'PASS' : 'FAIL',
    };
  });

  const columnsPassed = results.filter((r) => r.status === 'PASS').length;
  const columnsFailed = results.filter((r) => r.status === 'FAIL').length;
  const totalColumnsValidated = results.length;
  const passRate =
    totalColumnsValidated === 0
      ? '0.0'
      : ((columnsPassed / totalColumnsValidated) * 100).toFixed(1);

  return {
    results,
    summary: {
      totalColumnsValidated,
      columnsPassed,
      columnsFailed,
      passRate,
    },
  };
}

/**
 * Validate the request body for the schema-null-check endpoint.
 *
 * @param {Object} body
 * @returns {{ valid: boolean, error: string|null }}
 */
function validateRequestParams(body) {
  const { sourceTable, targetTable, primaryKey } = body || {};

  if (!sourceTable || !targetTable || !primaryKey) {
    return {
      valid: false,
      error: 'sourceTable, targetTable, and primaryKey are required',
    };
  }

  if (!TABLE_FORMAT_RE.test(sourceTable)) {
    return {
      valid: false,
      error: `Invalid source table format: ${sourceTable}. Must be project.dataset.table`,
    };
  }

  if (!TABLE_FORMAT_RE.test(targetTable)) {
    return {
      valid: false,
      error: `Invalid target table format: ${targetTable}. Must be project.dataset.table`,
    };
  }

  return { valid: true, error: null };
}

module.exports = {
  compareSchemas,
  identifyNotNullColumns,
  buildNullValidationResults,
  validateRequestParams,
};
