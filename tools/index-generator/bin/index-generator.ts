import { processPods } from '../lib'
import { resolve } from 'node:path'
import { podsPath } from './index-generator.json'

const path: string = resolve(podsPath)

console.log(`Processing pods from ${path}`)

processPods(path)
