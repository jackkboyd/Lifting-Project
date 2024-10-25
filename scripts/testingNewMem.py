workoutPkValues = {'MovementSequence': 1}

pk_values = {col: workoutPkValues.get(col,0) for col in workoutPkValues}
print(pk_values)

checkQuery = (f'Select "IDCOLUMN" from lift."TABLENAME" where "CODECOLUMN" = :codeValue')
print(checkQuery)

uniqueConditions = [(f'"{pk}" = :{pk}') for pk in pk_values.keys()]
print(uniqueConditions)

uniqueConditionsClause = 'AND '.join(uniqueConditions)
print(uniqueConditionsClause)

#join uniqueConditions to checkQuery

checkQuery = (f'{checkQuery} AND {uniqueConditionsClause}')
print(checkQuery)
