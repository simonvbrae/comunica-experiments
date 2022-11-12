import { testQuery } from '../lib/Query.js'

const configs = {
  /*
  'normal': {
    config: 'templates/config-query.json',
    query: '../ldbc-snb-decentralized.js/out-queries/interactive-short-1.sparql',
    profile: 'http://localhost:3000/pods/00000000000000000065/profile/card',
    cardinality: 'cardinalities/00000000000000000065.nt',
    pod: '00000000000000000065'
  },
  */
  'with-cardinality': {
    config: 'templates/config-query-cardinalities.json',
    query: '../ldbc-snb-decentralized.js/out-queries/interactive-short-1.sparql',
    profile: 'http://localhost:3000/pods/00000000000000000065/profile/card',
    cardinality: 'cardinalities/00000000000000000065.nt',
    pod: '00000000000000000065'
  }
}

const executionTimes = new Map()
const repeat = 3

for (let i = 0; i < repeat; i++) {
  for (const [name, configuration] of Object.entries(configs)) {
    const duration = await testQuery(configuration.config, configuration.query, configuration.profile, configuration.cardinality)
    executionTimes.set(name, [...(executionTimes.get(name) ?? []), duration])
  }
}

console.log(executionTimes)
