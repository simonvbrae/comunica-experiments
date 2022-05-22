import type { IActionRdfMetadataExtract, IActorRdfMetadataExtractOutput, IActorRdfMetadataExtractArgs } from '@comunica/bus-rdf-metadata-extract'
import { ActorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract'
import { IActorTest, ActionContextKey } from '@comunica/core'
import { KeysQueryOperation } from '@comunica/context-entries'
import * as RDF from '@rdfjs/types'

/**
 * An RDF Metadata Extract Actor that extracts total items counts from a metadata stream based on the given predicates.
 */
export class ActorRdfMetadataExtractContextCardinality extends ActorRdfMetadataExtract implements IActorRdfMetadataExtractArgs {
  public constructor(args: IActorRdfMetadataExtractArgs) {
    super(args)
  }

  public async test(action: IActionRdfMetadataExtract): Promise<IActorTest> {
    return true
  }

  public run(action: IActionRdfMetadataExtract): Promise<IActorRdfMetadataExtractOutput> {
    return new Promise((resolve, reject) => {
      // The cardinalities data is assumed to be found in the context, for testing purposes only
      const cardinalities: Map<string, number> = action.context.getSafe<Map<string, number>>(new ActionContextKey<Map<string, number>>('cardinalities'))
      //console.log(`Total ${cardinalities.size} entries`)

      // For the current quad, fetch the cardinality of the predicate (only)
      const quad: RDF.Quad = action.context.getSafe(KeysQueryOperation.operation) as RDF.Quad
      const estimate: number = cardinalities.get(quad.predicate.value) ?? Number.POSITIVE_INFINITY

      resolve({ metadata: { cardinality: { type: 'estimate', value: estimate } } })
    })
  }
}
