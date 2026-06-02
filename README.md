# Multipoint Messaging with Naming Service

Distributed systems – PP 1.3  
Equipe: Rafael, Lya, Igor

## Visão Geral

Extensão do trabalho anterior (PP 1.2) com a adição de um **Serviço de Nomes** simples (sem hierarquia, sem replicação). O Serviço de Nomes elimina toda configuração estática de endereços: **o único endereço fixo permitido nos peers é o do próprio Serviço de Nomes**.

O Serviço de Nomes acumula também a função de **Serviço de Diretório**, substituindo o Group Manager da versão anterior: a operação `discover("peer")` retorna todos os peers registrados e seus endereços.

---

## Arquitetura

```
naming-service:50050          (único endereço estático conhecidos pelos peers)
       │
       ├─ bind / lookup / unbind / register / discover
       │
peer-1:50070 ←─── descobre peer-2..4 via discover("peer") ───→ peer-2:50070
peer-3:50070 ←──────────────────────────────────────────────→ peer-4:50070
```

Cada peer, ao iniciar:
1. Conecta ao Serviço de Nomes (endereço vem de `NAME_SERVICE_ADDRESS`).
2. Chama `bind(nome, "host:porta")` — registra seu endereço.
3. Chama `register(nome, "peer")` — informa seu tipo.
4. Sobe um servidor gRPC para receber mensagens de outros peers.
5. Periodicamente chama `discover("peer")` e envia mensagens aleatórias.
6. Ao ser encerrado (SIGTERM): chama `unbind(nome)` antes de sair.

---

## Serviço de Nomes — Interface gRPC

Definida em `proto/naming.proto`:

| Operação | Descrição |
|---|---|
| `bind(name, address)` | Cria registro nome→endereço. Retorna `ok` ou `error`. |
| `lookup(name)` | Retorna endereço associado ao nome, ou erro se não existir. |
| `unbind(name)` | Remove o nome e seu registro. |
| `register(name, type)` | Associa um tipo a um nome já registrado; erro se nome não existir. |
| `discover(type)` | Retorna lista de `{name, address}` do tipo indicado. |

---

## Estrutura do Projeto

```
.
├── proto/
│   ├── naming.proto        # interface do Serviço de Nomes
│   └── peer.proto          # interface de mensagens entre peers
├── naming_service/
│   ├── server.py           # implementação do Serviço de Nomes
│   └── Dockerfile
├── peer/
│   ├── peer.py             # nó peer: registra-se, descobre, envia mensagens
│   └── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Como Executar

### Pré-requisitos

- Docker e Docker Compose instalados.

### Subir tudo

```bash
docker compose up --build
```

### Observar logs de um peer específico

```bash
docker compose logs -f peer-1
```

### Simular saída e entrada de um peer

```bash
# remover peer-3 (unbind automático via SIGTERM)
docker compose stop peer-3

# re-adicionar
docker compose start peer-3
```

### Parar tudo

```bash
docker compose down
```

---

## Exemplo de Saída

**Serviço de Nomes:**
```
[NAMING-SVC] BIND    peer-1               peer-1:50070
[NAMING-SVC] REGISTER peer-1              type=peer
[NAMING-SVC] DISCOVER type=peer       4 result(s)
```

**Peer:**
```
[peer-1] Bound   peer-1  →  peer-1:50070
[peer-1] Registered peer-1 as type=peer
[peer-1] → peer-3               "Hello from peer-1! (token=4217)"
[peer-1] ← peer-2               "Hello from peer-2! (token=8831)"
```

---

## Decisões de Projeto

- **Middleware:** gRPC (Python), alinhado com os trabalhos anteriores do grupo.
- **Configuração:** variáveis de ambiente no `docker-compose.yml`. O único endereço estático em qualquer peer é `NAME_SERVICE_ADDRESS: "naming-service:50050"`.
- **Sem Group Manager:** `discover("peer")` substitui completamente o papel do Group Manager.
- **Desligamento gracioso:** cada peer captura `SIGTERM` e chama `unbind` antes de encerrar, mantendo o registro consistente.
- **Rebind:** se um peer com o mesmo nome se reconectar, `bind` sobrescreve o endereço anterior sem erro.
