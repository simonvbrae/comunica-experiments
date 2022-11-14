import { join, resolve } from 'node:path'
import { readFileSync } from 'node:fs'
import { executeQuery } from '../lib/Query'

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

async function runQueryBasedOnConfiguration(config: IQueryConfiguration): Promise<void> {
  await executeQuery(config.config, config.query, config.seed)
  console.log('Finished running')
}

const configPath: string = join('templates', 'config-query-runner.json')
const configs: IQueryConfiguration[] = loadConfiguration(configPath)

if (!process.argv.includes('--idp')) {
  process.argv.push('--idp', 'void')
}

runQueryBasedOnConfiguration(configs[0])
  .then(() => console.log('Success!'))
  .catch((reason) => console.log('Fail!', reason))
