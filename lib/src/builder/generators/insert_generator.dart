import '../../core/case_style.dart';
import '../elements/column/column_element.dart';
import '../elements/column/field_column_element.dart';
import '../elements/column/foreign_column_element.dart';
import '../elements/column/join_column_element.dart';
import '../elements/column/reference_column_element.dart';
import '../elements/table_element.dart';
import '../utils.dart';

class InsertGenerator {
  String generateInsertMethod(TableElement table) {
    var deepInserts = <String>[];

    for (var column in table.columns
        .whereType<ReferenceColumnElement>()
        .where((c) => c.linkedTable.primaryKeyColumn == null)) {
      if (column.linkedTable.columns
          .where((c) =>
              c is ForeignColumnElement &&
              c.linkedTable != table &&
              !c.isNullable)
          .isNotEmpty) {
        continue;
      }

      var isNullable = column.isNullable;
      if (!column.isList) {
        var requestParams = column.linkedTable.columns
            .whereType<ParameterColumnElement>()
            .map((c) {
          if (c is ForeignColumnElement) {
            if (c.linkedTable == table) {
              if (table.primaryKeyColumn!.isAutoIncrement) {
                return '${c.paramName}: result[requests.indexOf(r)]';
              } else {
                return '${c.paramName}: r.${table.primaryKeyColumn!.paramName}';
              }
            } else {
              return '${c.paramName}: null';
            }
          } else {
            return '${c.paramName}: r.${column.paramName}${isNullable ? '!' : ''}.${c.paramName}';
          }
        });

        var deepInsert = '''
          await db.${column.linkedTable.repoName}.insertMany(requests${isNullable ? '.where((r) => r.${column.paramName} != null)' : ''}.map((r) {
            return ${column.linkedTable.element.name}InsertRequest(${requestParams.join(', ')});
          }).toList());
        ''';

        deepInserts.add(deepInsert);
      } else {
        var requestParams = column.linkedTable.columns
            .whereType<ParameterColumnElement>()
            .map((c) {
          if (c is ForeignColumnElement) {
            if (c.linkedTable == table) {
              if (table.primaryKeyColumn!.isAutoIncrement) {
                return '${c.paramName}: result[requests.indexOf(r)]';
              } else {
                return '${c.paramName}: r.${table.primaryKeyColumn!.paramName}';
              }
            } else {
              return '${c.paramName}: null';
            }
          } else {
            return '${c.paramName}: rr.${c.paramName}';
          }
        });

        var deepInsert = '''
          await db.${column.linkedTable.repoName}.insertMany(requests${isNullable ? '.where((r) => r.${column.paramName} != null)' : ''}.expand((r) {
            return r.${column.paramName}${isNullable ? '!' : ''}.map((rr) => ${column.linkedTable.element.name}InsertRequest(${requestParams.join(', ')}));
          }).toList());
        ''';

        deepInserts.add(deepInsert);
      }
    }

    final isAutoIncrementPrimaryKey =
        table.primaryKeyColumn?.isAutoIncrement ?? false;

    var insertColumns = table.columns
        .whereType<NamedColumnElement>()
        .where((c) => c is! FieldColumnElement || !c.isAutoIncrement);

    String toInsertValue(
      NamedColumnElement c, {
      String prefix = 'r.',
      String valuesFieldName = 'values',
    }) {
      if (c.converter != null) {
        return '\${$valuesFieldName.add(${c.converter!.toSource()}.tryEncode($prefix${c.paramName}))}::${c.rawSqlType}';
      } else {
        return '\${$valuesFieldName.add($prefix${c.paramName}${c.converter != null ? ', ${c.converter!.toSource()}' : ''})}::${c.rawSqlType}';
      }
    }

    var manyToManyInserts = <String>[];

    for (var joinColumn in table.columns.whereType<JoinColumnElement>()) {
      if (table.primaryKeyColumn == null) {
        throw 'ManyToMany relation with missing primary keys!';
      }
      String insertValuesFieldName = CaseStyle.camelCase
          .transform('insert_${joinColumn.joinTable.tableName}_values');
      // For simplicity and to remove deleted relations
      // Delete all join table entries for this table
      // Insert all many to many relations for this table into join table
      String manyToManyInsert;
      // TODO WHAT IF RELATION LIST IS EMPTY?!?!? -> STILL VALID SQL THAT SIMPLY DOES NOTHING OR CRASH?!?!?!
      if (isAutoIncrementPrimaryKey) {
        // TODO this will have to retireve primary key from previous query not requests
        manyToManyInsert = '''
          if (r.${joinColumn.paramName}.isNotEmpty) {
            var $insertValuesFieldName = QueryValues();
            await db.query(
              'INSERT INTO "${joinColumn.joinTable.tableName}" ("${joinColumn.parentTable.getForeignKeyName()!}", "${joinColumn.linkedTable.getForeignKeyName()!}")\\n'
              'VALUES \${r.${joinColumn.paramName}.map((${joinColumn.linkedTable.tableName}_${joinColumn.linkedTable.primaryKeyColumn!.paramName}) => '(${toInsertValue(joinColumn.parentTable.primaryKeyColumn!, valuesFieldName: insertValuesFieldName, prefix: '')}, ${toInsertValue(joinColumn.linkedTable.primaryKeyColumn!, valuesFieldName: insertValuesFieldName, prefix: '${joinColumn.linkedTable.tableName}_')})').join(', ')}',
              $insertValuesFieldName.values,
            );
          }
        ''';
      } else {
        manyToManyInsert = '''
          if(requests.any((r) => r.${joinColumn.paramName}.isNotEmpty)) {
            var $insertValuesFieldName = QueryValues();
            await db.query(
              'INSERT INTO "${joinColumn.joinTable.tableName}" ("${joinColumn.parentTable.getForeignKeyName()!}", "${joinColumn.linkedTable.getForeignKeyName()!}")\\n'
              'VALUES \${requests.expand((r) => r.${joinColumn.paramName}.map((${joinColumn.linkedTable.tableName}_${joinColumn.linkedTable.primaryKeyColumn!.paramName}) => '(${toInsertValue(joinColumn.parentTable.primaryKeyColumn!, valuesFieldName: insertValuesFieldName)}, ${toInsertValue(joinColumn.linkedTable.primaryKeyColumn!, valuesFieldName: insertValuesFieldName, prefix: '${joinColumn.linkedTable.tableName}_')})')).join(', ')}',
              $insertValuesFieldName.values,
            );
          }
        ''';
      }
      manyToManyInserts.add(manyToManyInsert);
    }

    String? autoIncrementStatement, keyReturnStatement;

    if (isAutoIncrementPrimaryKey) {
      var name = table.primaryKeyColumn!.columnName;
      if (manyToManyInserts.isEmpty) {
        autoIncrementStatement = '''
          var result = rows.map<int>((r) => TextEncoder.i.decode(r.toColumnMap()['$name'])).toList();
        ''';
      } else {
        autoIncrementStatement = '''
          // var result = rows.map<int>((r) => TextEncoder.i.decode(r.toColumnMap()['$name'])).toList();
          var ${table.primaryKeyColumn!.paramName} = TextEncoder.i.decode<int>(rows.first.toColumnMap()['$name']);
          result.add(${table.primaryKeyColumn!.paramName});
        ''';
      }

      keyReturnStatement = 'return result;';
    }

    var insert = '';
    // Simply non for in insert in case no ManyToMany or no autoIncrement
    if (manyToManyInserts.isNotEmpty && isAutoIncrementPrimaryKey) {
      // This means that there are manyToMany relations to be inserted and the primary key is not given in insert model -> needs to be retrieved from DB
      insert = '''
        var result = <int>[];
        for (var r in requests) {
          var values = QueryValues();
          ${autoIncrementStatement != null ? 'var rows = ' : ''}await db.query(
            'INSERT INTO "${table.tableName}" ( ${insertColumns.map((c) => '"${c.columnName}"').join(', ')} )\\n'
            'VALUES ( ${insertColumns.map(toInsertValue).join(', ')} )\\n'
            ${autoIncrementStatement != null ? "'RETURNING \"${table.primaryKeyColumn!.columnName}\"'" : ''},
            values.values,
          );
          ${autoIncrementStatement ?? ''}
          ${manyToManyInserts.isNotEmpty ? manyToManyInserts.join() : ''}
        }
      ''';
    } else {
      insert = '''
        var values = QueryValues();
        ${autoIncrementStatement != null ? 'var rows = ' : ''}await db.query(
          'INSERT INTO "${table.tableName}" ( ${insertColumns.map((c) => '"${c.columnName}"').join(', ')} )\\n'
          'VALUES \${requests.map((r) => '( ${insertColumns.map(toInsertValue).join(', ')} )').join(', ')}\\n'
          ${autoIncrementStatement != null ? "'RETURNING \"${table.primaryKeyColumn!.columnName}\"'" : ''},
          values.values,
        );
        ${autoIncrementStatement ?? ''}
        ${manyToManyInserts.isNotEmpty ? manyToManyInserts.join() : ''}
      ''';
    }

    return '''
      @override
      Future<${keyReturnStatement != null ? 'List<int>' : 'void'}> insert(List<${table.element.name}InsertRequest> requests) async {
        if (requests.isEmpty) return${keyReturnStatement != null ? ' []' : ''};
        /*var values = QueryValues();
        ${autoIncrementStatement != null ? 'var rows = ' : ''}await db.query(
          'INSERT INTO "${table.tableName}" ( ${insertColumns.map((c) => '"${c.columnName}"').join(', ')} )\\n'
          'VALUES \${requests.map((r) => '( ${insertColumns.map(toInsertValue).join(', ')} )').join(', ')}\\n'
          ${autoIncrementStatement != null ? "'RETURNING \"${table.primaryKeyColumn!.columnName}\"'" : ''},
          values.values,
        );
        ${autoIncrementStatement ?? ''}
        ${manyToManyInserts.isNotEmpty ? manyToManyInserts.join() : ''}
        ${deepInserts.isNotEmpty ? deepInserts.join() : ''}*/
        $insert
        ${keyReturnStatement ?? ''}
      }
    ''';
  }

  String generateInsertRequest(TableElement table) {
    var requestClassName = '${table.element.name}InsertRequest';
    var requestFields = <MapEntry<String, String>>[];

    for (var column in table.columns) {
      if (column is FieldColumnElement) {
        if (!column.isAutoIncrement) {
          requestFields.add(MapEntry(
              column.parameter.type.getDisplayString(withNullability: true),
              column.paramName));
        }
      } else if (column is ReferenceColumnElement &&
          column.linkedTable.primaryKeyColumn == null) {
        if (column.linkedTable.columns
            .where((c) =>
                c is ForeignColumnElement &&
                c.linkedTable != table &&
                !c.isNullable)
            .isNotEmpty) {
          continue;
        }
        requestFields.add(MapEntry(
            column.parameter!.type.getDisplayString(withNullability: true),
            column.paramName));
      } else if (column is ForeignColumnElement) {
        var fieldNullSuffix = column.isNullable ? '?' : '';
        String fieldType;
        if (column.linkedTable.primaryKeyColumn == null) {
          fieldType = column.linkedTable.element.name;
          if (column.isList) {
            fieldType = 'List<$fieldType>';
          }
        } else {
          fieldType = column.linkedTable.primaryKeyColumn!.dartType;
        }
        requestFields
            .add(MapEntry('$fieldType$fieldNullSuffix', column.paramName));
      } else if (column is JoinColumnElement) {
        // TODO default empty array
        if (column.linkedTable.primaryKeyParameter == null) {
          throw 'ManyToMany relation with missing primary keys!';
        }
        requestFields.add(MapEntry(
          'List<${column.linkedTable.primaryKeyParameter!.type.getDisplayString(withNullability: false)}>',
          column.paramName,
        ));
      }
    }

    var constructorParameters = requestFields
        .map((f) => '${f.key.endsWith('?') ? '' : 'required '}this.${f.value},')
        .join(' ');

    return '''
      ${defineClassWithMeta(requestClassName, table.meta?.read('insert'))}
        $requestClassName(${constructorParameters.isNotEmpty ? '{$constructorParameters}' : ''});
        
        ${requestFields.map((f) => '${f.key} ${f.value};').join('\n')}
      }
    ''';
  }
}
