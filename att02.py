import pika
import threading
import time
import json

class PedidoProducer(threading.Thread):
    def __init__(self, connection):
        super().__init__()
        self.connection = connection
        self.channel = connection.channel()
        self.channel.queue_declare(queue='pedidos')

    def run(self):
        while True:
            # Simulando um pedido sendo feito pelo cliente
            cliente_id = input("Digite o ID do cliente: ")
            produto = input("Digite o produto a ser comprado: ")
            quantidade = input("Digite a quantidade desejada: ")
            
            pedido = {
                'cliente_id': cliente_id,
                'produto': produto,
                'quantidade': quantidade
            }

            # Convertendo o pedido para JSON antes de enviá-lo para a fila
            mensagem = json.dumps(pedido)
            self.channel.basic_publish(exchange='', routing_key='pedidos', body=mensagem)
            print("Pedido enviado:", mensagem)

class PedidoConsumer(threading.Thread):
    def __init__(self, connection):
        super().__init__()
        self.connection = connection
        self.channel = connection.channel()
        self.channel.queue_declare(queue='pedidos')

    def callback(self, ch, method, properties, body):
        pedido = json.loads(body)
        print("Pedido recebido:", pedido)
        # Simulando o processamento do pedido
        print("Pedido sendo processado para o cliente", pedido['cliente_id'], "...")
        time.sleep(3)  # Simula o processamento do pedido
        print("Pedido para o cliente", pedido['cliente_id'], "processado com sucesso!")

    def run(self):
        self.channel.basic_consume(queue='pedidos', on_message_callback=self.callback, auto_ack=True)
        print("Aguardando pedidos...")
        self.channel.start_consuming()

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    
    produtor = PedidoProducer(connection)
    consumidor = PedidoConsumer(connection)

    produtor.start()
    consumidor.start()

    produtor.join()
    consumidor.join()

    connection.close()

if __name__ == "__main__":
    main()
