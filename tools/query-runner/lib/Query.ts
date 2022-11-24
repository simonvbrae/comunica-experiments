import { QueryEngine, QueryEngineFactory } from '@comunica/query-sparql-link-traversal-solid'
import { type QueryStringContext } from '@comunica/types'
import { resolve, basename, extname } from 'node:path'
import { readFileSync, writeFileSync } from 'node:fs'

interface IRunnerConfigurationFile {
  configs: string[]
  queries: string[]
  results: string
  repeat?: number
}

interface IRunnerConfiguration {
  configs: string[]
  queries: Map<string, string[]>
  results: string
  repeat: number
}

interface IExecutionResult {
  query: string
  config: string
  resultBindings: number
  totalExecutions: number
  averageDuration: number
}

const queryEngineFactory: QueryEngineFactory = new QueryEngineFactory()
const seedUrlRegex = /<(http:\/\/localhost:3000\/pods\/.*)>/

function loadQueriesFromFile(path: string): string[] {
  const queries: string[] = readFileSync(path, 'utf8').split('\n\n') // assumes \n\n only between queries!
  return queries
}

function loadConfiguration(path: string): IRunnerConfiguration {
  const runnerConfigurationFile: IRunnerConfigurationFile = JSON.parse(readFileSync(path, 'utf8')) as IRunnerConfigurationFile
  const runnerConfiguration: IRunnerConfiguration = {
    configs: runnerConfigurationFile.configs.map((path) => resolve(path)),
    queries: new Map<string, string[]>(runnerConfigurationFile.queries.map((path) => [
      path, loadQueriesFromFile(resolve(path))
    ])),
    results: resolve(runnerConfigurationFile.results),
    repeat: runnerConfigurationFile.repeat ?? 1
  }
  return runnerConfiguration
}

function extractSeedUrlsFromQuery(query: string): string[] {
  const results = seedUrlRegex.exec(query)
  if (results != null) {
    return [...results]
  } else {
    throw new Error('No seed URLs found in query')
  }
}

async function executeQuery(query: string, queryPath: string, queryIndex: number, configPath: string, repeat: number): Promise<IExecutionResult> {
  const seedUrls: string[] = extractSeedUrlsFromQuery(query)
  const context: QueryStringContext = { sources: [seedUrls[0], ...seedUrls.slice(1)], lenient: true }
  const durations: number[] = []

  let resultCount: number | undefined

  for (let i=0; i<repeat; i++) {
    const queryEngine: QueryEngine = await queryEngineFactory.create({ configPath: configPath })

    const timeStart: number = Date.now()
    const results = await (await queryEngine.queryBindings(query, context)).toArray()
    const timeEnd: number = Date.now()

    if (resultCount != null && results.length != resultCount) {
      throw new Error('Different result count for an execution!')
    } else if (resultCount == null) {
      resultCount = results.length
    }

    durations.push(timeEnd - timeStart)
  }

  const executionResult: IExecutionResult = {
    averageDuration: Math.round(durations.reduce((accumulator, duration) => accumulator + duration) / durations.length),
    query: `${basename(queryPath).replace(extname(queryPath), '')}-${queryIndex}`,
    config: basename(configPath).replace(extname(configPath), ''),
    resultBindings: resultCount as number,
    totalExecutions: repeat
  }

  console.log(`Execute: ${executionResult.query} in average ${executionResult.averageDuration} ms over ${executionResult.totalExecutions} executions with ${executionResult.resultBindings} results using ${executionResult.config}`)

  return executionResult
}

function serializeExecutionResults(path: string, results: IExecutionResult[]): void {
  const separator = ','
  const resultKeys: string[] = Object.keys(results[0]).sort((a, b) => a.localeCompare(b))
  const outputLines: string[] = [resultKeys.join(separator)]
  for (const result of results) {
    const resultValues: Map<string, string> = new Map<string, string>(Object.entries(result).map((value) => [value[0], new String(value[1]).toString()]))
    outputLines.push(resultKeys.map((key) => resultValues.get(key)).join(separator))
  }
  writeFileSync(path, outputLines.join('\n'), { encoding: 'utf8' })
}

async function executeQueriesFromConfiguration(configPath: string): Promise<void> {
  const runnerConfiguration: IRunnerConfiguration = loadConfiguration(configPath)
  const executionResults: IExecutionResult[] = []
  for (const [queryPath, queryStrings] of runnerConfiguration.queries) {
    for (let queryIndex=0; queryIndex<queryStrings.length; queryIndex++) {
      for (const config of runnerConfiguration.configs) {
        const executionResult = await executeQuery(queryStrings[queryIndex], queryPath, queryIndex, config, runnerConfiguration.repeat)
        executionResults.push(executionResult)
      }
    }
  }
  serializeExecutionResults(runnerConfiguration.results, executionResults)
}

export { executeQueriesFromConfiguration }
