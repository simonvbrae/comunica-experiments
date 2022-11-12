import { QueryEngine, QueryEngineFactory } from '@comunica/query-sparql-link-traversal-solid'
import { Parser } from 'n3'
import { readFileSync } from 'node:fs'

async function testQuery(configFile: string, queryFile: string, solidProfileUri: string, cardinalityFilePath: string): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    console.log(`Config: ${configFile}`)
    console.log(`Query: ${queryFile}`)
    console.log(`Profile: ${solidProfileUri}`)
    console.log(`Cardinality: ${cardinalityFilePath}`)

    const queryEngineFactory: QueryEngineFactory = new QueryEngineFactory()

    const query: string = readFileSync(queryFile).toString().split('\n\n')[0]

    const parser: Parser = new Parser()
    const cardinalityFileContents: string = readFileSync(cardinalityFilePath).toString()
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

    queryEngineFactory.create({ configPath: configFile }).then((queryEngine: QueryEngine) => {
      const startTimeApprox: Date = new Date()
      let count = 0

      queryEngine
        .queryBindings(query, {
          sources: [solidProfileUri],
          cardinalities: cardinalityData
        })
        .then((bindingsStream) => {
          bindingsStream
            .on('data', (binding: any) => {
              count++
              /*
            for (const [key, value] of binding.entries.entries()) {
              console.log(`${key}: ${value.value}`)
            }
            console.log()
            */
            })
            .on('end', () => {
              const endTimeApprox: Date = new Date()
              const totalMilliseconds: number = endTimeApprox.getTime() - startTimeApprox.getTime()
              console.log(`Finished in approximately ${totalMilliseconds} ms, ${count} results`)
              resolve(totalMilliseconds)
            })
        })
    })
  })
}

export { testQuery }
