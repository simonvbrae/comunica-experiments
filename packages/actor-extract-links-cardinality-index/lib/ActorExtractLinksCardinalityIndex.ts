import type { ActorInitQueryBase } from '@comunica/actor-init-query'
import { QueryEngineBase } from '@comunica/actor-init-query'
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf'
import type { IActionExtractLinks, IActorExtractLinksOutput } from '@comunica/bus-extract-links'
import { ActorExtractLinks } from '@comunica/bus-extract-links'
//import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links'
import { KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries'
import { ActionContextKey } from '@comunica/core'
import type { IActorArgs, IActorTest } from '@comunica/core'
import type { IActionContext } from '@comunica/types'
import type * as RDF from '@rdfjs/types'
import { storeStream } from 'rdf-store-stream'

/**
 * A comunica Cardinality Index Extract Links Actor.
 */
export class ActorExtractLinksCardinalityIndex extends ActorExtractLinks {
  //public static readonly RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'

  private readonly cardinalityIndexPredicates: string[]
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf
  public readonly queryEngine: QueryEngineBase

  public constructor(args: IActorExtractLinksCardinalityIndexArgs) {
    super(args)
    this.queryEngine = new QueryEngineBase(args.actorInitQuery)
  }

  public async test(action: IActionExtractLinks): Promise<IActorTest> {
    if (!action.context.get(KeysInitQuery.query)) {
      throw new Error(`Actor ${this.name} can only work in the context of a query.`)
    }
    if (!action.context.get(KeysQueryOperation.operation)) {
      throw new Error(`Actor ${this.name} can only work in the context of a query operation.`)
    }
    return true
  }

  public async run(action: IActionExtractLinks): Promise<IActorExtractLinksOutput> {
    // Determine links to type indexes
    const cardinalityIndexes = await this.extractCardinalityIndexLinks(action.metadata)

    // Dereference all type indexes, and collect them in one record
    const cardinalities = (await Promise.all(cardinalityIndexes.map((cardinalityIndex) => this.dereferenceCardinalityIndex(cardinalityIndex, action.context))))
      .reduce<Record<string, number>>((acc, cardinalities) => {
      for (const [id, cardinality] of Object.entries(cardinalities)) {
        acc[id] += cardinality
      }
      return acc
    }, {})

    // TODO: save the cardinalities somewhere here?
    action.context.set(new ActionContextKey<Map<string, number>>('cardinalities'), cardinalities)

    // ... :(
    return { links: [] }
  }

  /**
   * Extract links to cardinality index from the metadata stream.
   * @param metadata A metadata quad stream.
   */
  public extractCardinalityIndexLinks(metadata: RDF.Stream): Promise<string[]> {
    return new Promise<string[]>((resolve, reject) => {
      const cardinalityIndexesInner: string[] = []

      // Forward errors
      metadata.on('error', reject)

      // Invoke callback on each metadata quad
      metadata.on('data', (quad: RDF.Quad) => {
        if (this.cardinalityIndexPredicates.includes(quad.predicate.value)) {
          cardinalityIndexesInner.push(quad.object.value)
        }
      })

      // Resolve to discovered links
      metadata.on('end', () => {
        resolve(cardinalityIndexesInner)
      })
    })
  }

  /**
   * Determine all entries within the given cardinality index.
   * @param cardinalityIndex The URL of a cardinality index.
   * @param context The context.
   * @return cardinalityMap A record mapping class URLs to an array of links.
   */
  public async dereferenceCardinalityIndex(cardinalityIndex: string, context: IActionContext): Promise<Map<string, number>> {
    // Parse the type index document
    const response = await this.mediatorDereferenceRdf.mediate({ url: cardinalityIndex, context })
    const store = await storeStream(response.data)

    // Query the document to extract all type registrations
    const bindingsArray = await (
      await this.queryEngine.queryBindings(
        `
        PREFIX owl: <http://www.w3.org/2002/07/owl#>

        SELECT ?id ?cardinality WHERE {
          ?id owl:cardinality ?cardinality .
        }`,
        { sources: [store] }
      )
    ).toArray()

    // Collect links per type
    const cardinalityMap: Map<string, number> = new Map<string, number>()

    for (const bindings of bindingsArray) {
      const id: string | undefined = bindings.get('id')?.value
      const cardinality: string | undefined = bindings.get('cardinality')?.value
      if (id && cardinality) {
        cardinalityMap.set(id, parseInt(cardinality, 10))
      }
    }

    return cardinalityMap
  }
}

export interface IActorExtractLinksCardinalityIndexArgs extends IActorArgs<IActionExtractLinks, IActorTest, IActorExtractLinksOutput> {
  /**
   * The cardinality index predicate URLs that will be followed.
   * @default {http://example.org/terms#publicCardinalityIndex}
   * @default {http://example.org/terms#privateCardinalityIndex}
   */
  cardinalityIndexPredicates: string[]
  /**
   * An init query actor that is used to query shapes.
   * @default {<urn:comunica:default:init/actors#query>}
   */
  actorInitQuery: ActorInitQueryBase
  /**
   * The Dereference RDF mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf
}
