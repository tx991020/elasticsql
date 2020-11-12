package mongo

//ldoc.add(generateLookupStep(tholder, joinTableName, joinTableAlias, j.getOnExpression(), whereExpHolder.getExpression(), subqueryDocs));
//ldoc.add(generateUnwindStep(tholder, joinTableAlias, j.isLeft()));

func toPipelineSteps() {

}

func generateInternalLookup(left, right, leftCol, rightCol string) map[string]interface{} {

	m := map[string]interface{}{}
	m["from"] = right
	m["localField"] = leftCol
	m["foreignField"] = rightCol
	m["as"] = right
	return m

}




