import { testCardinalitiesByPod } from '../lib/index'
import { cardinalitiesPath, podsPath } from '../../../templates/config-cardinality-counter.json'

testCardinalitiesByPod(cardinalitiesPath, podsPath)
