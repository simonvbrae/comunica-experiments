import { processPods } from '../lib'
import { resolve } from 'node:path'

const path: string = resolve('out-fragments', 'http', 'localhost_3000', 'pods')

processPods(path)
