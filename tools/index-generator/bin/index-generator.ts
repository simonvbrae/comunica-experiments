import { processPods } from '../lib'
import { join } from 'node:path'
import { readFileSync } from 'node:fs'

interface IConfiguration {
    pods: string
}

const configPath: string = join('templates', 'config-index-generator.json')
const config: IConfiguration = JSON.parse(readFileSync(configPath, 'utf8')) as IConfiguration

console.log(`Processing pods from ${config.pods}`)

processPods(config.pods)
