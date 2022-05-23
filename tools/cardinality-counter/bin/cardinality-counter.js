import { calculateCardinalitiesByPod } from '../lib/index.js'
import config from './cardinality-counter.json' assert { type: 'json' }

calculateCardinalitiesByPod(config.cardinalitiesPath, config.podsPath)
