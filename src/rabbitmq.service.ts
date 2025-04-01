import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  ClientProxy,
  ClientProxyFactory,
  MicroserviceOptions,
  Transport,
} from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';

interface ClientInfo {
  client: ClientProxy;
  connected: boolean;
}

@Injectable()
export class RabbitMQService implements OnApplicationShutdown {
  private clients: Map<string, ClientInfo> = new Map();
  constructor(private readonly configService: ConfigService) {}

  // 클라이언트 생성 및 재사용
  private async createClient(queue: string): Promise<ClientProxy> {
    if (this.clients.has(queue)) {
      const { client, connected } = this.clients.get(queue)!;

      // 연결 상태를 확인하고 재연결
      if (!connected) {
        await this.reconnectClient(queue, client);
      }
      return client;
    }

    const options: MicroserviceOptions = {
      transport: Transport.RMQ,
      options: {
        urls: [this.configService.get<string>('RABBITMQ_URL') || 'amqp://localhost:5672'],
        queue,
        queueOptions: { durable: false }, // 큐를 내구성 있게 설정
        prefetchCount: 10, // 한 번에 처리할 메시지 수 제한
        noAck: true, // ACK 필요 설정
      },
    };

    const client = ClientProxyFactory.create(options);
    this.clients.set(queue, { client, connected: true });

    return client;
  }

  private async reconnectClient(queue: string, client: ClientProxy): Promise<void> {
    try {
      await client.connect();
      this.clients.set(queue, { client, connected: true });
      console.log(`"${queue}" 대기열에 다시 연결되었습니다.`);
    } catch (error) {
      console.error(`"${queue}" 대기열에 다시 연결하지 못했습니다:`, error);
    }
  }

  // 메시지 발행 (Emit)
  async emit(queue: string, pattern: string, payload: any): Promise<void> {
    try {
      const client = await this.createClient(queue);
      await firstValueFrom(client.emit(queue, { pattern, payload }));
    } catch (error) {
      console.error(
        `"${pattern}" 패턴을 사용하여 "${queue}" 대기열에 메시지를 내보내는 중 오류가 발생했습니다:`,
        error,
      );
      throw error;
    }
  }

	// 요청-응답 (Send) with Retry
	async send(queue: string, pattern: string, payload: any, retries = 3, backoff = 300): Promise<any> {
		for (let attempt = 1; attempt <= retries + 1; attempt++) {
			try {
				const client = await this.createClient(queue);
				return await firstValueFrom(client.send(queue, { pattern, payload }));
			} catch (error:any) {
				console.error(
					`"${pattern}" 패턴을 사용하여 "${queue}" 대기열에 메시지를 보내는 중 오류가 발생했습니다 (시도 ${attempt}/${retries + 1}):`,
					error.message || error,
				);

				// 마지막 시도였는지 확인
				if (attempt > retries) {
					console.error(`최대 재시도 횟수 초과. 요청 실패: ${pattern}`);
					throw error;
				}

				// 점진적으로 대기 시간 증가 (300ms, 600ms, 1200ms...)
				const delay = backoff * Math.pow(2, attempt - 1);
				console.log(`${delay}ms 후 재시도...`);
				await new Promise(resolve => setTimeout(resolve, delay));
			}
		}
	}

  // 애플리케이션 종료 시 모든 연결 닫기
  async onApplicationShutdown(): Promise<void> {
    for (const [queue, { client }] of this.clients.entries()) {
      try {
        client.close();
        console.log(`"${queue}" 대기열의 클라이언트가 닫혔습니다.`);
      } catch (error) {
        console.error(`"${queue}" 대기열의 클라이언트를 닫는 동안 오류가 발생했습니다:`, error);
      }
      this.clients.delete(queue);
    }
    console.log('모든 RabbitMQ 클라이언트가 종료되었습니다.');
  }
}
