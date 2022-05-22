'use strict'

import { getCardinalities, serializeCardinalities } from './CardinalityCounter'
import { join } from 'node:path'
import { readdirSync } from 'node:fs'
import { WriterOptions } from 'n3'

function testCardinalitiesByPod(cardinalitiesPath: string, podsPath: string) {
  console.log(`Pods path: ${podsPath}`)
  console.log(`Cardinalities path: ${cardinalitiesPath}`)
  const writerOptions: WriterOptions = { format: 'N-Quads' }
  const pods: Array<string> = readdirSync(podsPath)
  console.log(`Reading pods from: ${podsPath}`)

  const handlePod = function (podName: string) {
    getCardinalities(join(podsPath, podName)).then((cardinalityData) => {
      serializeCardinalities(cardinalitiesPath, podName, cardinalityData, writerOptions).then((path) => {
        console.log(`Wrote for pod ${podName} to ${path}`)
        pods.pop()
        if (pods.length > 0) {
          handlePod(pods.at(-1) || '')
        }
      })
    })
  }

  handlePod(pods.at(-1) || '')
}

export { testCardinalitiesByPod }
