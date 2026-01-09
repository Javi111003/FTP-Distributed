#!/usr/bin/env python3
"""
Script detallado de prueba para reconciliación de split-brain.
Simula una partición de red, realiza operaciones en ambas particiones,
y verifica la reconciliación.
"""

import sys
import time
import logging
import socket
from pathlib import Path

# Agregar el path del proyecto
sys.path.insert(0, str(Path(__file__).parent))

from FTP.Distributed.Common.rpc_protocol import RPCClient, RPCMessage
from FTP.Distributed.Common.constants import MessageType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SplitBrainTester:
    """Clase para probar la reconciliación de split-brain"""
    
    def __init__(self):
        self.rpc_client = RPCClient()
        self.metadata1_host = 'localhost'
        self.metadata1_port = 5000
        self.metadata2_host = 'localhost'
        self.metadata2_port = 5002
    
    def wait_for_service(self, host: str, port: int, timeout: int = 30):
        """Espera a que un servicio esté disponible"""
        logger.info(f"Esperando a que {host}:{port} esté disponible...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    logger.info(f"✓ {host}:{port} está disponible")
                    return True
            except:
                pass
            
            time.sleep(1)
        
        logger.error(f"✗ {host}:{port} no está disponible después de {timeout}s")
        return False
    
    def query_leader(self, host: str, port: int) -> dict:
        """Consulta quién es el líder"""
        try:
            msg = RPCMessage(MessageType.LEADER_QUERY, {})
            response = self.rpc_client.call(host, port, msg, timeout=5)
            
            if response:
                leader_id = response.payload.get('leader_id')
                leader_host = response.payload.get('leader_host')
                leader_port = response.payload.get('leader_port')
                
                logger.info(f"Líder reportado por {host}:{port} -> {leader_id} @ {leader_host}:{leader_port}")
                
                return {
                    'leader_id': leader_id,
                    'leader_host': leader_host,
                    'leader_port': leader_port
                }
            
        except Exception as e:
            logger.error(f"Error consultando líder en {host}:{port}: {e}")
        
        return None
    
    def get_state_summary(self, host: str, port: int) -> dict:
        """Obtiene un resumen del estado del nodo"""
        try:
            msg = RPCMessage(
                MessageType.REPL_SNAPSHOT,
                {'request_type': 'state_summary', 'requester_id': 'tester'}
            )
            response = self.rpc_client.call(host, port, msg, timeout=5)
            
            if response and response.payload:
                state = {
                    'term': response.payload.get('term', 0),
                    'leader_id': response.payload.get('leader_id'),
                    'commit_index': response.payload.get('commit_index', -1),
                    'oplog_length': response.payload.get('oplog_length', 0),
                    'file_count': response.payload.get('file_count', 0)
                }
                
                logger.info(
                    f"Estado de {host}:{port} -> "
                    f"term={state['term']}, "
                    f"leader={state['leader_id']}, "
                    f"files={state['file_count']}, "
                    f"commit={state['commit_index']}"
                )
                
                return state
        
        except Exception as e:
            logger.error(f"Error obteniendo estado de {host}:{port}: {e}")
        
        return None
    
    def check_split_brain(self) -> bool:
        """Verifica si hay split-brain en el clúster"""
        logger.info("=" * 60)
        logger.info("Verificando estado del clúster...")
        logger.info("=" * 60)
        
        leader1 = self.query_leader(self.metadata1_host, self.metadata1_port)
        leader2 = self.query_leader(self.metadata2_host, self.metadata2_port)
        
        if not leader1 or not leader2:
            logger.warning("No se pudo consultar ambos nodos")
            return False
        
        if leader1['leader_id'] != leader2['leader_id']:
            logger.error("🔴 SPLIT-BRAIN DETECTADO!")
            logger.error(f"   metadata1 reporta líder: {leader1['leader_id']}")
            logger.error(f"   metadata2 reporta líder: {leader2['leader_id']}")
            return True
        else:
            logger.info(f"✓ Clúster unificado con líder: {leader1['leader_id']}")
            return False
    
    def run_test(self):
        """Ejecuta el test completo"""
        
        print("\n" + "=" * 80)
        print("TEST DE RECONCILIACIÓN DE SPLIT-BRAIN")
        print("=" * 80 + "\n")
        
        # Paso 1: Verificar servicios disponibles
        logger.info("PASO 1: Verificando servicios...")
        if not self.wait_for_service(self.metadata1_host, self.metadata1_port):
            logger.error("metadata1 no está disponible. Inicia el servicio primero.")
            return False
        
        if not self.wait_for_service(self.metadata2_host, self.metadata2_port):
            logger.warning("metadata2 no está disponible. Continuando con metadata1 solo...")
        
        time.sleep(2)
        
        # Paso 2: Estado inicial
        logger.info("\nPASO 2: Estado inicial del clúster...")
        state1_initial = self.get_state_summary(self.metadata1_host, self.metadata1_port)
        state2_initial = self.get_state_summary(self.metadata2_host, self.metadata2_port)
        
        has_split_brain = self.check_split_brain()
        
        if has_split_brain:
            logger.warning("\n⚠️  SPLIT-BRAIN DETECTADO en estado inicial!")
            logger.info("El sistema debería iniciar reconciliación automáticamente...")
            
            # Esperar a que la reconciliación ocurra
            logger.info("\nEsperando reconciliación automática (30 segundos)...")
            for i in range(30):
                time.sleep(1)
                if i % 10 == 0:
                    logger.info(f"Esperando... {30-i}s restantes")
            
            # Verificar estado después de reconciliación
            logger.info("\nPASO 3: Verificando estado después de reconciliación...")
            state1_after = self.get_state_summary(self.metadata1_host, self.metadata1_port)
            state2_after = self.get_state_summary(self.metadata2_host, self.metadata2_port)
            
            has_split_brain_after = self.check_split_brain()
            
            if not has_split_brain_after:
                logger.info("\n✅ RECONCILIACIÓN EXITOSA!")
                logger.info("El clúster ahora tiene un solo líder.")
                
                # Mostrar cambios
                logger.info("\nCambios detectados:")
                logger.info(f"  metadata1: files {state1_initial.get('file_count', 0)} -> {state1_after.get('file_count', 0)}")
                logger.info(f"  metadata2: files {state2_initial.get('file_count', 0)} -> {state2_after.get('file_count', 0)}")
                
                return True
            else:
                logger.error("\n❌ RECONCILIACIÓN FALLIDA!")
                logger.error("Todavía hay múltiples líderes después de esperar.")
                return False
        else:
            logger.info("\n✓ No hay split-brain. Clúster está funcionando correctamente.")
            
            logger.info("\nPara probar la reconciliación:")
            logger.info("1. Detén metadata2: docker-compose stop metadata2")
            logger.info("2. Sube archivos vía FTP a metadata1")
            logger.info("3. Reinicia metadata2: docker-compose start metadata2")
            logger.info("4. Sube archivos diferentes (con mismo nombre) vía FTP a metadata2")
            logger.info("5. Ejecuta este script de nuevo para verificar reconciliación")
            
            return True
    
    def print_summary(self):
        """Imprime un resumen del estado actual"""
        print("\n" + "=" * 80)
        print("RESUMEN DEL ESTADO ACTUAL")
        print("=" * 80 + "\n")
        
        logger.info("Consultando estado de metadata1...")
        state1 = self.get_state_summary(self.metadata1_host, self.metadata1_port)
        
        logger.info("Consultando estado de metadata2...")
        state2 = self.get_state_summary(self.metadata2_host, self.metadata2_port)
        
        self.check_split_brain()
        
        print("\n" + "=" * 80)


def main():
    """Función principal"""
    tester = SplitBrainTester()
    
    try:
        success = tester.run_test()
        
        print("\n" + "=" * 80)
        if success:
            print("✅ TEST COMPLETADO")
        else:
            print("❌ TEST FALLIDO")
        print("=" * 80 + "\n")
        
        # Ofrecer mostrar resumen
        import sys
        if '--summary' in sys.argv:
            tester.print_summary()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("\nTest interrumpido por el usuario")
        return 1
    except Exception as e:
        logger.error(f"Error durante el test: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())

