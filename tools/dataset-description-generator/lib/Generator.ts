import { type ISolidPod, SolidPod } from './SolidPod'
import { join } from 'node:path'
import { readdirSync } from 'node:fs'

async function processPodsFromPath(path: string): Promise<void> {
  console.log(`Process pods from ${path}`)
  const pods: ISolidPod[] = readdirSync(path).map((pod) => join(path, pod)).map((podPath) => new SolidPod(podPath))
  await Promise.all(pods.map((pod) => pod.process()))
}

function processPods(path: string): void {
  processPodsFromPath(path)
    .then(() => console.log('Finished!'))
    .catch((reason) => console.log('Failed:', reason))
}

export { processPods }
