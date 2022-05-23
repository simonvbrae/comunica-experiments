'use strict'

import { getCardinalities, serializeCardinalities } from './CardinalityCounter.js'
import { join, resolve } from 'node:path'
import { readdirSync, existsSync, mkdirSync, rmSync } from 'node:fs'
import { WriterOptions } from 'n3'

function calculateCardinalitiesByPod(cardinalitiesPath: string, podsPath: string) {
  console.log(`Pods: ${resolve(podsPath)}`)
  console.log(`Cardinalities: ${resolve(cardinalitiesPath)}`)

  if (existsSync(cardinalitiesPath)) {
    console.log(`Deleting old cardinalities`)
    rmSync(cardinalitiesPath, { force: true, recursive: true })
  }
  mkdirSync(cardinalitiesPath)

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

export { calculateCardinalitiesByPod }
