import { join } from 'node:path'
import { executeQueriesFromConfiguration } from '../lib/Query'

const configPath: string = join('templates', 'config-query-runner.json')

executeQueriesFromConfiguration(configPath)
  .then(() => console.log('Success!'))
  .catch((reason) => console.log('Fail!', reason))
