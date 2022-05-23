import { testQuery } from '../lib/index.js'
import configs from './query-runner.json' assert { type: 'json' }

const executionTimes = new Map()
const repeat = 3

for (let i = 0; i < repeat; i++) {
  for (const [name, configuration] of Object.entries(configs)) {
    const duration = await testQuery(configuration.config, configuration.query, configuration.profile, configuration.cardinality)
    executionTimes.set(name, [...(executionTimes.get(name) ?? []), duration])
  }
}

console.log(executionTimes)
