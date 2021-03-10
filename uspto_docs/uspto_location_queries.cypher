--Location of Inventor
g.V().has('Location', 'state_fips', '18')
	 .inE('Inventor_Located_In')
	 .outV()
	 .outE('Inventor_Of')
	 .inV()
	 .valueMap().limit(10);

--Location of Assignee
g.V().has('Location', 'state_fips', '18')
	 .inE('Assignee_Located_In')
	 .outV()
	 .inE('Assigned_To')
	 .outV()
	 .valueMap().limit(10);
