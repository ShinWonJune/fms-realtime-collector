import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FMSDataGenerator:
    def __init__(self, kafka_brokers='localhost:9092', topic='fms-data'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers.split(','),
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.topic = topic
        logger.info(f"FMS Data Generator initialized for topic: {topic}")
    
    def generate_sample_data(self):
        """샘플 FMS 데이터 생성"""
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'vehicle_id': f"FMS_{random.randint(1000, 9999)}",
            'driver_id': f"DRV_{random.randint(100, 999)}",
            'location': {
                'latitude': round(37.5665 + random.uniform(-0.1, 0.1), 6),  # 서울 기준
                'longitude': round(126.9780 + random.uniform(-0.1, 0.1), 6)
            },
            'speed': round(random.uniform(0, 120), 2),
            'fuel_level': round(random.uniform(10, 100), 2),
            'engine_temp': round(random.uniform(80, 110), 1),
            'mileage': random.randint(10000, 300000),
            'status': random.choice(['driving', 'idle', 'parked', 'maintenance']),
            'alerts': random.choice([[], ['low_fuel'], ['high_temp'], ['speed_limit']])
        }
    
    def send_data(self, data):
        """Kafka로 데이터 전송"""
        try:
            future = self.producer.send(
                self.topic,
                key=data['vehicle_id'],
                value=data
            )
            future.get(timeout=10)
            return True
        except Exception as e:
            logger.error(f"Failed to send data: {e}")
            return False
    
    def run_continuous(self, interval=5):
        """지속적으로 데이터 생성 및 전송"""
        logger.info(f"Starting continuous data generation (interval: {interval}s)")
        
        try:
            while True:
                data = self.generate_sample_data()
                if self.send_data(data):
                    logger.info(f"Sent data for vehicle {data['vehicle_id']}")
                else:
                    logger.error("Failed to send data")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Data generation stopped by user")
        except Exception as e:
            logger.error(f"Error in data generation: {e}")
        finally:
            self.producer.close()
    
    def generate_historical_data(self, hours_back=24, records_per_minute=10):
        """과거 데이터 생성 (백필 테스트용)"""
        logger.info(f"Generating historical data for last {hours_back} hours")
        
        current_time = datetime.now(timezone.utc)
        start_time = current_time - timedelta(hours=hours_back)
        
        total_records = 0
        
        while start_time < current_time:
            for _ in range(records_per_minute):
                data = self.generate_sample_data()
                # 타임스탬프를 과거 시간으로 설정
                data['timestamp'] = start_time.isoformat()
                
                if self.send_data(data):
                    total_records += 1
            
            start_time += timedelta(minutes=1)
            
            if total_records % 1000 == 0:
                logger.info(f"Generated {total_records} historical records")
        
        logger.info(f"Historical data generation completed. Total records: {total_records}")

if __name__ == "__main__":
    import argparse
    from datetime import timedelta
    
    parser = argparse.ArgumentParser(description='FMS Data Generator')
    parser.add_argument('--mode', choices=['continuous', 'historical'], 
                       default='continuous', help='Generation mode')
    parser.add_argument('--kafka-brokers', default='localhost:9092', 
                       help='Kafka broker addresses')
    parser.add_argument('--topic', default='fms-data', 
                       help='Kafka topic name')
    parser.add_argument('--interval', type=int, default=5, 
                       help='Interval between messages (seconds)')
    parser.add_argument('--hours-back', type=int, default=24, 
                       help='Hours of historical data to generate')
    
    args = parser.parse_args()
    
    generator = FMSDataGenerator(args.kafka_brokers, args.topic)
    
    if args.mode == 'continuous':
        generator.run_continuous(args.interval)
    elif args.mode == 'historical':
        generator.generate_historical_data(args.hours_back)
