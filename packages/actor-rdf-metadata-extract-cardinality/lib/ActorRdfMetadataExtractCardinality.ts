import { type IActionRdfMetadataExtract, type IActorRdfMetadataExtractOutput, type IActorRdfMetadataExtractArgs, ActorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract'
import { type ActorInitQueryBase } from '@comunica/actor-init-query'
import { QueryEngine } from '@comunica/query-sparql-link-traversal-solid'
import { type IActorTest } from '@comunica/core'
import { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf'
import { KeysQueryOperation, KeysInitQuery } from '@comunica/context-entries'
// import { storeStream } from 'rdf-store-stream'
import * as RDF from '@rdfjs/types'

/**
 * An RDF Metadata Extract Actor that attempts to retrieve cardinalities for the data from a custom index.
 */
export class ActorRdfMetadataExtractCardinality extends ActorRdfMetadataExtract implements IActorRdfMetadataExtractCardinalityArgs {

  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf
  public readonly actorInitQuery: ActorInitQueryBase

  private readonly queryEngine: QueryEngine // this thing looks weird, nesting Comunica in itself...

  private static readonly predicateCardinalitiesByDataset: Map<string, Map<string, number>> = new Map<string, Map<string, number>>()

  public constructor(args: IActorRdfMetadataExtractCardinalityArgs) {
    super(args)
    this.actorInitQuery = args.actorInitQuery
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf
    this.queryEngine = new QueryEngine(args.actorInitQuery)
  }

  public async test(action: IActionRdfMetadataExtract): Promise<IActorTest> {
    if (!action.context.get(KeysInitQuery.query)) {
      throw new Error(`Actor ${this.name} can only work in the context of a query.`)
    }
    if (!action.context.get(KeysQueryOperation.operation)) {
      throw new Error(`Actor ${this.name} can only work in the context of a query operation.`)
    }
    return true
  }

  public run(action: IActionRdfMetadataExtract): Promise<IActorRdfMetadataExtractOutput> {
    return new Promise((resolve, reject) => {
      const quad: RDF.Quad = action.context.getSafe(KeysQueryOperation.operation) as RDF.Quad
      this.getCardinalityForPredicate(action, quad.predicate.value)
        .then((cardinality) => {
          resolve({ metadata: { cardinality: { type: 'estimate', value: cardinality } } })
        })
        .catch((reason) => {
          console.log(reason)
          reject(reason)
        })
    })
  }

  private async getCardinalityForPredicate(action: IActionRdfMetadataExtract, predicate: string): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      if (!this.cardinalityDataExistsForUrl(action.url)) {
        this.retrieveCardinalityDataForDataset(action)
          .then(() => resolve(this.extractCardinalityforPredicate(action.url, predicate)))
          .catch((reason) => {
            console.log(reason)
            reject(reason)
          })
      }
      resolve(this.extractCardinalityforPredicate(action.url, predicate))
    })
  }

  private async retrieveCardinalityDataForDataset(action: IActionRdfMetadataExtract): Promise<void> {
    // console.log('Attempt to dereference:', action.url)

    //const response = await this.mediatorDereferenceRdf.mediate({ url: action.url, context: action.context })
    //const store = await storeStream(response.data)

    const query = `
      PREFIX void: <http://rdfs.org/ns/void#>

      SELECT ?dataset ?property ?propertyCardinality WHERE {
        ?dataset a void:Dataset ;
          void:propertyPartition [
            ?property ?propertyCardinality
          ] .
      }
    `

    const bindingsStream = await this.queryEngine.queryBindings(query, { sources: [action.url], lenient: true })
    const bindingsArray: RDF.Bindings[] = await bindingsStream.toArray()

    for (const bindings of bindingsArray) {
      console.log(bindings)
      const dataset = bindings.get('dataset')
      const property = bindings.get('property')
      const propertyCardinality = bindings.get('propertyCardinality')
      if (dataset && property && propertyCardinality) {
        let datasetData = ActorRdfMetadataExtractCardinality.predicateCardinalitiesByDataset.get(dataset.value)
        if (!datasetData) { // this is unnecessary for all bindings except the first...
          datasetData = new Map<string, number>()
          ActorRdfMetadataExtractCardinality.predicateCardinalitiesByDataset.set(dataset.value, datasetData)
        }
        datasetData.set(property.value, (datasetData.get(property.value) ?? 0) + parseInt(propertyCardinality.value))
      }
    }

    console.log('predicateCardinalitiesByDataset', ActorRdfMetadataExtractCardinality.predicateCardinalitiesByDataset)
  }

  private cardinalityDataExistsForUrl(url: string): boolean {
    for (const key of ActorRdfMetadataExtractCardinality.predicateCardinalitiesByDataset.keys()) {
      if (url.startsWith(key)) {
        return true
      }
    }
    return false
  }

  private extractCardinalityforPredicate(url: string, predicate: string): number {
    for (const [key, data] of ActorRdfMetadataExtractCardinality.predicateCardinalitiesByDataset) {
      if (url.startsWith(key)) {
        return data.get(predicate) ?? Number.POSITIVE_INFINITY
      }
    }
    return Number.POSITIVE_INFINITY
  }
}

export interface IActorRdfMetadataExtractCardinalityArgs extends IActorRdfMetadataExtractArgs {
  /**
   * An init query actor that is used to query shapes.
   * @default {<urn:comunica:default:init/actors#query>}
   */
  actorInitQuery: ActorInitQueryBase
  /**
   * The Dereference RDF mediator
   * @default {<urn:comunica:default:dereference-rdf/mediators#main>}
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf
}
