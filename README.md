# Azure Functions - Order Processing System

Projeto desenvolvido para praticar arquitetura orientada a eventos utilizando Azure Functions v4 com Python Model v2.

O sistema simula o processamento assíncrono de pedidos de um e-commerce utilizando filas, persistência em Table Storage e mecanismos de retry automáticos do Azure Functions.

---

## 📚 Conceitos praticados

- Azure Functions v4
- Python Programming Model v2
- HTTP Trigger
- Queue Trigger
- Queue Output Binding
- Table Output Binding
- Processamento assíncrono
- Arquitetura orientada a eventos (EDA)
- Retry automático
- Poison Queue
- Idempotência
- Validação de payload com JSON Schema

---

## 🏗️ Arquitetura

```text
HTTP Request
    ↓
receiving_products (HTTP Trigger)
    ↓
pending-orders (Queue)
    ↓
verify_purchase_order (Queue Trigger)
    ├── approvedorders (Table Storage)
    ├── analyse-orders (Queue)
    └── refused-orders (Queue)
                    ↓
        send_email_analyse_order
                    ↓
analyseordersemailsending (Table Storage)
```

---

## ⚙️ Fluxo do sistema

### 1. Recebimento do pedido

A Function HTTP recebe um pedido em formato JSON, realiza validações básicas do payload e adiciona a mensagem na fila `pending-orders`.

### 2. Processamento assíncrono

A Function `verify_purchase_order` consome a fila e:

- calcula o valor total do pedido
- classifica o pedido
- roteia o fluxo conforme regras de negócio

### 3. Regras de negócio

| Condição     | Ação               |
| ------------ | ------------------ |
| Total > 5000 | Envia para análise |
| Total <= 0   | Rejeita pedido     |
| Total válido | Aprova pedido      |

---

## 🧪 Exemplo de payload

```json
{
  "order_id": "ORD-001",
  "customer": "Gui",
  "items": [
    {
      "product": "Notebook",
      "price": 3500,
      "quantity": 1
    },
    {
      "product": "Mouse",
      "price": 150,
      "quantity": 2
    }
  ]
}
```

---

## 🔁 Retry e Poison Queue

O projeto utiliza o comportamento padrão de retry do Azure Queue Trigger.

Quando ocorre uma exceção durante o processamento:

- a mensagem retorna para a fila
- o Azure tenta processar novamente
- após múltiplas falhas, a mensagem é movida automaticamente para a poison queue

Exemplo:

```text
pending-orders-poison
```

---

## 🛡️ Idempotência

Para evitar duplicidade no armazenamento, o sistema utiliza `order_id` como `RowKey` no Azure Table Storage.

Isso permite tratar reprocessamentos de mensagens de forma consistente.

---

## 🚀 Tecnologias utilizadas

- Python 3
- Azure Functions v4
- Azure Storage Account
- Azure Queue Storage
- Azure Table Storage
- JSON Schema

---

## ▶️ Executando localmente

### Pré-requisitos

- Python 3.10+
- Azure Functions Core Tools
- Azurite (opcional)
- VS Code com extensão Azure Functions

---

### Instalação

```bash
pip install -r requirements.txt
```

---

### Executar projeto

```bash
func start
```

---

## 📌 Objetivo do projeto

O foco deste projeto não é criar um sistema completo de e-commerce, mas praticar conceitos fundamentais de sistemas distribuídos e arquiteturas orientadas a eventos utilizando serviços serverless da Azure.

---

## 📖 Aprendizados

Durante o desenvolvimento foram explorados conceitos importantes como:

- desacoplamento
- tolerância a falhas
- processamento assíncrono
- retries automáticos
- poison queue
- consistência de dados
- responsabilidades em pipelines orientados a eventos

---
