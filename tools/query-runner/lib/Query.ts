import { QueryEngine, QueryEngineFactory } from '@comunica/query-sparql-link-traversal-solid'
import { resolve } from 'node:path'
import { readFileSync } from 'node:fs'

// import { KeysInitQuery } from '@comunica/context-entries'
// import { readFileSync } from 'node:fs'
// import { CliArgsHandlerSolidNoAuth } from './CliArgsHandlerSolidNoAuth'

interface IQueryConfiguration {
  config: string
  query: string
  seed: string
}

function loadConfiguration(path: string): IQueryConfiguration[] {
  const configData: IQueryConfiguration[] = JSON.parse(readFileSync(path, 'utf8')) as IQueryConfiguration[]
  for (const config of configData) {
    config.config = resolve(config.config)
    config.query = resolve(config.query)
  }
  return configData
}

async function executeQueriesFromConfiguration(configPath: string): Promise<void> {
  const configurations: IQueryConfiguration[] = loadConfiguration(configPath)
  for (const configuration of configurations) {
    await executeQuery(configuration.config, configuration.query, configuration.seed)
  }
}

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

    const results = await (await queryEngine.queryBindings(query, {
      sources: [ seedUrl ],
      lenient: false
      /*
      [KeysInitQuery.cliArgsHandlers.name]: [
        new CliArgsHandlerSolidNoAuth()
      ]*/
    })).toArray()

    const approximateEndTime: Date = new Date()
    const approximateDuration: number = approximateEndTime.getTime() - approximateStartTime.getTime()
  
    console.log(`Query took approximately ${approximateDuration} ms and produced ${results.length} triples`)
  }

  console.log('Finished')
}

export { executeQueriesFromConfiguration }
