import { QueryEngine, QueryEngineFactory } from '@comunica/query-sparql-link-traversal-solid'
import { Parser } from 'n3'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

class QuerySource {
  solidProfile: string
  cardinalityFile: string

  constructor(id: string) {
    this.solidProfile = `http://localhost:3000/pods/${id}/profile/card`
    this.cardinalityFile = join('cardinalities', `${id}.nt`)
  }
}

async function testQuery() {
  const configPath: string = join('templates', 'config-query.json')
  const queryPath: string = join('..', 'ldbc-snb-decentralized.js', 'out-queries', 'interactive-short-4.sparql')

  const queryEngineFactory: QueryEngineFactory = new QueryEngineFactory()
  //const queryStrings: string[] = await readFileSync(queryPath).toString().split('\n\n')

  /*
  ?person snvoc:hasInterest ?interest .

  ?interest foaf:name ?interestName .
  */

  const source: QuerySource = new QuerySource('00000000000000000065')

  const queryStrings: string[] = [
    `
    PREFIX snvoc: <http://localhost:3000/www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?interest ?interestName WHERE {
      ?person snvoc:hasInterest ?interest .
      ?interest foaf:name ?interestName .
    } LIMIT 100
    `
  ]

  const parser: Parser = new Parser()
  const cardinalityFileContents: string = readFileSync(source.cardinalityFile).toString()
  const cardinalityData: Map<string, number> = new Map<string, number>()

  parser.parse(cardinalityFileContents, (error, quad) => {
    if (error) {
      throw error
    }
    if (quad) {
      //console.log(quad.toJSON())
      cardinalityData.set(quad.subject.value, parseInt(quad.object.value, 10))
    }
  })

  const queryEngine: QueryEngine = await queryEngineFactory.create({ configPath: configPath })

  for (const queryString of queryStrings) {
    await queryEngine
      .queryBindings(queryString, {
        sources: [source.solidProfile],
        cardinalities: cardinalityData
      })
      .then((bindingsStream) => {
        bindingsStream
          .on('data', (binding) => {
            for (const [key, value] of binding.entries.entries()) {
              console.log(`${key}: ${value.value}`)
            }
            console.log()
          })
          .on('end', () => console.log('Finished!'))
      })
  }
}

export { testQuery }
