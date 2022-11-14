import { QueryEngine, QueryEngineFactory } from '@comunica/query-sparql-link-traversal-solid'
// import { readFileSync } from 'node:fs'

async function executeQuery(configPath: string, queryPath: string, seedUrl: string): Promise<void> {
  console.log(`Execute query from ${queryPath}`)
  console.log(`Using configuration at ${configPath}`)
  console.log(`With seed URL: ${seedUrl}`)

  const queryEngineFactory: QueryEngineFactory = new QueryEngineFactory()

  // const queries: string[] = readFileSync(queryPath, 'utf8').split('}\n\n').map((query) => query + '}\n')
  const queries: string[] = [
    'SELECT * WHERE { ?s ?p ?o } LIMIT 10'
  ]

  const queryEngine: QueryEngine = await queryEngineFactory.create({ configPath: configPath })

  for (const query of queries) {
    // console.log(`Execute query:\n\n${query}\n`)

    const approximateStartTime: Date = new Date()
    const results = await (await queryEngine.queryBindings(query, { sources: [ seedUrl ], lenient: false, idp: 'void' })).toArray()
    const approximateEndTime: Date = new Date()
    const approximateDuration: number = approximateEndTime.getTime() - approximateStartTime.getTime()
  
    console.log(`Query took approximately ${approximateDuration} ms and produced ${results.length} triples`)
  }

  console.log('Finished')
}

export { executeQuery }
