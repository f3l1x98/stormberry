import 'package:analyzer/dart/element/type.dart';

import '../../core/case_style.dart';
import '../elements/column/column_element.dart';
import '../elements/column/field_column_element.dart';
import '../elements/column/foreign_column_element.dart';
import '../elements/column/join_column_element.dart';
import '../elements/column/reference_column_element.dart';
import '../elements/table_element.dart';
import '../utils.dart';

class UpdateGenerator {
  String generateUpdateMethod(TableElement table) {
    var deepUpdates = <String>[];

    print("UPDATE GENERATOR FOR ${table.tableName}");

    // For each OneToMany relation field (aka for each List<OtherTable> excluding ManyToMany)
    for (var column in table.columns
        .whereType<ReferenceColumnElement>()
        .where((c) => c.linkedTable.primaryKeyColumn == null)) {
      // Get all non nullable foreign key of referenced table that do NOT reference this table
      // If NOT empty -> skip (TODO REASON?!?!?)
      if (column.linkedTable.columns
          .where((c) =>
              c is ForeignColumnElement &&
              c.linkedTable != table &&
              !c.isNullable)
          .isNotEmpty) {
        continue;
      }

      if (!column.isList) {
        var requestParams = <String>[];
        for (var c
            in column.linkedTable.columns.whereType<ParameterColumnElement>()) {
          if (c is ForeignColumnElement) {
            if (c.linkedTable == table) {
              requestParams.add(
                  '${c.paramName}: r.${table.primaryKeyColumn!.paramName}');
            }
          } else {
            requestParams
                .add('${c.paramName}: r.${column.paramName}!.${c.paramName}');
          }
        }

        var deepUpdate = '''
          await db.${column.linkedTable.repoName}.updateMany(requests.where((r) => r.${column.paramName} != null).map((r) {
            return ${column.linkedTable.element.name}UpdateRequest(${requestParams.join(', ')});
          }).toList());
        ''';

        deepUpdates.add(deepUpdate);
      } else {
        var requestParams = <String>[];
        for (var c
            in column.linkedTable.columns.whereType<ParameterColumnElement>()) {
          if (c is ForeignColumnElement) {
            if (c.linkedTable == table) {
              requestParams.add(
                  '${c.paramName}: r.${table.primaryKeyColumn!.paramName}');
            }
          } else {
            requestParams.add('${c.paramName}: rr.${c.paramName}');
          }
        }

        var deepUpdate = '''
          await db.${column.linkedTable.repoName}.updateMany(requests.where((r) => r.${column.paramName} != null).expand((r) {
            return r.${column.paramName}!.map((rr) => ${column.linkedTable.element.name}UpdateRequest(${requestParams.join(', ')}));
          }).toList());
        ''';

        deepUpdates.add(deepUpdate);
      }
    }

    var hasPrimaryKey = table.primaryKeyColumn != null;
    // Get all columns that have to receive an update value (aka all non primaryKey and non autoIncrement ones)
    var setColumns = table.columns.whereType<NamedColumnElement>().where((c) =>
        (hasPrimaryKey
            ? c != table.primaryKeyColumn
            : c is FieldColumnElement) &&
        (c is! FieldColumnElement || !c.isAutoIncrement));

    // All that are the foreignKey, not of FieldColumnElement type or not auto increment
    var updateColumns = table.columns.whereType<NamedColumnElement>().where(
        (c) =>
            table.primaryKeyColumn == c ||
            c is! FieldColumnElement ||
            !c.isAutoIncrement);

    String toUpdateValue(
      NamedColumnElement c, {
      String prefix = 'r.',
      String valuesFieldName = 'values',
    }) {
      if (c.converter != null) {
        return '\${$valuesFieldName.add(${c.converter!.toSource()}.tryEncode($prefix${c.paramName}))}::${c.rawSqlType}';
      } else {
        return '\${$valuesFieldName.add($prefix${c.paramName})}::${c.rawSqlType}';
      }
    }

    String whereClause;

    if (hasPrimaryKey) {
      whereClause =
          '"${table.tableName}"."${table.primaryKeyColumn!.columnName}" = UPDATED."${table.primaryKeyColumn!.columnName}"';
    } else {
      whereClause = table.columns
          .whereType<ForeignColumnElement>()
          .map((c) =>
              '"${table.tableName}"."${c.columnName}" = UPDATED."${c.columnName}"')
          .join(' AND ');
    }

    var manyToManyUpdates = <String>[];

    for (var joinColumn in table.columns.whereType<JoinColumnElement>()) {
      if (!hasPrimaryKey) {
        throw 'ManyToMany relation with missing primary keys!';
      }
      String insertValuesFieldName = CaseStyle.camelCase
          .transform('insert_${joinColumn.joinTable.tableName}_values');
      String deleteValuesFieldName = CaseStyle.camelCase
          .transform('delete_${joinColumn.joinTable.tableName}_values');
      // For simplicity and to remove deleted relations
      // Delete all join table entries for this table
      // Insert all many to many relations for this table into join table
      manyToManyUpdates.add('''
        var $deleteValuesFieldName = QueryValues();
        await db.query(
          'DELETE FROM "${joinColumn.joinTable.tableName}"\\n'
          'WHERE \${requests.map((r) => '"${joinColumn.joinTable.tableName}"."${joinColumn.parentTable.getForeignKeyName()!}" = ${toUpdateValue(joinColumn.parentTable.primaryKeyColumn!, valuesFieldName: deleteValuesFieldName)}').join(' OR ')};',
          $deleteValuesFieldName.values,
        );
        if (requests.any((r) => r.${joinColumn.paramName}.isNotEmpty)) {
          var $insertValuesFieldName = QueryValues();
          await db.query(
            'INSERT INTO "${joinColumn.joinTable.tableName}" ("${joinColumn.parentTable.getForeignKeyName()!}", "${joinColumn.linkedTable.getForeignKeyName()!}")\\n'
            'VALUES \${requests.expand((r) => r.${joinColumn.paramName}.map((${joinColumn.linkedTable.tableName}_${joinColumn.linkedTable.primaryKeyColumn!.paramName}) => '(${toUpdateValue(joinColumn.parentTable.primaryKeyColumn!, valuesFieldName: insertValuesFieldName)}, ${toUpdateValue(joinColumn.linkedTable.primaryKeyColumn!, valuesFieldName: insertValuesFieldName, prefix: '${joinColumn.linkedTable.tableName}_')})')).join(', ')}',
            $insertValuesFieldName.values,
          );
        }
      ''');
    }

    return '''
        @override
        Future<void> update(List<${table.element.name}UpdateRequest> requests) async {
          if (requests.isEmpty) return;
          var values = QueryValues();
          await db.query(
            'UPDATE "${table.tableName}"\\n'
            'SET ${setColumns.map((c) => '"${c.columnName}" = COALESCE(UPDATED."${c.columnName}", "${table.tableName}"."${c.columnName}")').join(', ')}\\n'
            'FROM ( VALUES \${requests.map((r) => '( ${updateColumns.map(toUpdateValue).join(', ')} )').join(', ')} )\\n'
            'AS UPDATED(${updateColumns.map((c) => '"${c.columnName}"').join(', ')})\\n'
            'WHERE $whereClause',
            values.values,
          );
          ${manyToManyUpdates.isNotEmpty ? manyToManyUpdates.join() : ''}
          ${deepUpdates.isNotEmpty ? deepUpdates.join() : ''}
        }
      ''';
  }

  String generateUpdateRequest(TableElement table) {
    var requestClassName = '${table.element.name}UpdateRequest';
    var requestFields = <MapEntry<String, String>>[];

    for (var column in table.columns) {
      if (column is FieldColumnElement) {
        if (column == table.primaryKeyColumn || !column.isAutoIncrement) {
          requestFields.add(MapEntry(
            column.parameter.type.getDisplayString(withNullability: false) +
                (column == table.primaryKeyColumn ? '' : '?'),
            column.paramName,
          ));
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
            column.parameter!.type.getDisplayString(withNullability: false) +
                (column == table.primaryKeyColumn ? '' : '?'),
            column.paramName));
      } else if (column is ForeignColumnElement) {
        var fieldNullSuffix = column == table.primaryKeyColumn ? '' : '?';
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

    final constructorParameters = requestFields
        .map((f) => '${f.key.endsWith('?') ? '' : 'required '}this.${f.value},')
        .join(' ');

    return '''
      ${defineClassWithMeta(requestClassName, table.meta?.read('update'))}
        $requestClassName(${constructorParameters.isNotEmpty ? '{$constructorParameters}' : ''});
        
        ${requestFields.map((f) => '${f.key} ${f.value};').join('\n')}
      }
    ''';
  }
}
