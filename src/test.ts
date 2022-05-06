'use strict'

import { getCardinalities, serializeCardinalities } from './lib.js'
import { cwd } from 'node:process'
import { join } from 'node:path'
import { readdirSync } from 'node:fs'

/*
function testCardinalities() {
  const path: string = join(cwd(), 'ldbc_snb_ttl', 'social_network_activity_0_0.ttl')
  const options: ParserOptions = { blankNodePrefix: '' }

  getCardinalities(path, options).then((cardinalities: Map<string, number>) => {
    const cardinalitySum: number = Array.from(cardinalities.values()).reduce((acc, val) => acc + val)
    console.log(`Finished! Total of ${cardinalities.size} items, ${cardinalitySum} cardinality`)
  })
}

function testCardinalitiesRecursively() {
  const path: string = join(cwd(), 'ldbc_snb_ttl')
  const options: ParserOptions = { blankNodePrefix: '' }

  getCardinalitiesRecursively(path, options).then((cardinalities: Map<string, number>) => {
    const cardinalitySum: number = Array.from(cardinalities.values()).reduce((acc, val) => acc + val)
    console.log(`Finished recursive! Total of ${cardinalities.size} items, ${cardinalitySum} cardinality`)
  })
}
*/

function testCardinalitiesByPod() {
  const podsPath: string = join(cwd(), 'pods') // '..', 'ldbc-snb-decentralized.js', 'out-fragments', 'http', 'localhost_3000', 'pods')
  const cardinalitiesPath: string = join(cwd(), 'cardinalities')
  console.log(`Reading pods from: ${podsPath}`)
  for (const podName of readdirSync(podsPath)) {
    const singlePodPath: string = join(podsPath, podName)
    getCardinalities(singlePodPath).then((cardinalityData) => {
      serializeCardinalities(cardinalitiesPath, podName, cardinalityData).then((path) => console.log(`Wrote for pod ${podName}`))
    })
  }
}

export { testCardinalitiesByPod }
