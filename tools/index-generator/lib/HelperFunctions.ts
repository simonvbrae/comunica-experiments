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

  const podPromises: Promise<void>[] = pods.map(
    (podName) =>
      new Promise((resolve, reject) => {
        getCardinalities(join(podsPath, podName)).then((cardinalityData) => {
          serializeCardinalities(cardinalitiesPath, podName, cardinalityData, writerOptions).then((path) => {
            console.log(`Wrote for pod ${podName} to ${path}`)
            resolve()
          })
        })
      })
  )

  Promise.all(podPromises).then(() => console.log('Finished all pods!'))
}

export { calculateCardinalitiesByPod }
