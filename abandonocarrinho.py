from __future__ import absolute_import

import json
import datetime
import argparse

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

class JsonCoder(object):
  
  def encode(self, x):
    return json.dumps(x)

  def decode(self, x):
    return json.loads(x)

def filtrar(interacoes, carrinho, pagamento):
  
  """ 
  Avaliação do abandono de carrinho:
    - Relação das últimas interações do cliente com as páginas 'basket' e 'checkout'
    - Se a última transição 'basket' => 'checkout' levou mais do que 10 minutos
    - Se não houve interação com a página 'checkout' ao final da sessão 
  """
  
  interacao_carrinho = list(filter(lambda x: interacoes['customer'] in x, carrinho))
  interacao_pagamento = list(filter(lambda x: interacoes['customer'] in x, pagamento))

  data_interacao = datetime.datetime.strptime(interacoes['timestamp'],'%Y-%m-%d %H:%M:%S') 
  data_carrinho = [datetime.datetime.strptime(item[1],'%Y-%m-%d %H:%M:%S') for item in interacao_carrinho]
  data_pagamento = [datetime.datetime.strptime(item[1],'%Y-%m-%d %H:%M:%S') for item in interacao_pagamento]

  if(len(data_pagamento) > 0 and len(data_carrinho) > 0):
    intervalo = (data_pagamento[0] - data_carrinho[0]).total_seconds() / 60.0

  if((len(data_pagamento) <= 0 or intervalo > 10) and data_interacao == data_carrinho[0]):
      yield interacoes

def run(argv=None):
  
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', required=True, help='Arquivo de entrada', default='input/page-views.json')
  parser.add_argument('--output', required=True, help='Arquivo de saída', default='output/abandoned-carts')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=PipelineOptions()) as pipeline:

    interacoes = pipeline | "lê arquivo de entrada" >> ReadFromText(known_args.input, coder=JsonCoder())

    data_ultima_interacao_carrinho = (
          interacoes
          | "filtrar as interações com o carrinho" >> beam.Filter(lambda elem: elem['page'] == 'basket')
          | "criar key = customer e value = timestamp --car" >> beam.Map(lambda elem: (elem['customer'], elem['timestamp']))
          | "agrupar pela data da ultima interação --car" >> beam.CombinePerKey(max)
    )  
    
    data_ultima_interacao_pagamento = (
          interacoes
          | "filtrar as interações com o pagamento" >> beam.Filter(lambda elem: elem['page'] == 'checkout')
          | "criar key = customer e value = timestamp --pag" >> beam.Map(lambda elem: (elem['customer'], elem['timestamp']))
          | "agrupar pela data da ultima interação --pag" >> beam.CombinePerKey(max)
    )  
  
    resultado = (
        interacoes
        | "verificar quais interações resultaram em abandono de carrinho" >>
          beam.FlatMap(filtrar, carrinho=beam.pvalue.AsList(data_ultima_interacao_carrinho),pagamento=beam.pvalue.AsList(data_ultima_interacao_pagamento))
        | "encoder para formato .json" >> beam.Map(json.dumps)
        | "cria arquivo de saída" >> WriteToText(known_args.output,'.json')
    )

if __name__ == '__main__':
  
  run()
