module.exports = {
  title: "datwrapper",
  type: "object",
  properties: {
    title: {
      type: 'string',
      description: 'the title of the dataset'
    },
    description: {
      type: ['string', 'null'],
      description: 'the description of the dataset'
    },
    websites: {
      type: 'array',
      description: 'the websites where this dataset is used',
      items: {
        type: 'string'
      },
      default: []
    },
    created: {
      type: ['string']
    },
    updated: {
      type: ['string', 'null']
    }
  }  
}
