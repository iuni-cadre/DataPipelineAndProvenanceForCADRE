### MAG to OpenAlex

OpenAlex stands in as a replacement of OpenAlex.

### Process

1) Get a sample file transformed to parquet from JSON.

- This is because of how long JSON takes to load at the scale of OpenAlex

- Using ```jsonSchema.py``` against largest file of entity to get a good schema sample

2) Using Schema to load in all JSON and transform to parquet

- Using ```json2parquet2.py```

3) Run ```node.py``` to extract all TSVs for edges and nodes for OpenAlex