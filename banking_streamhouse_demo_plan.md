# Banking Streamhouse Demo Plan

## 1. Demo Objective

Demonstrate how a streamhouse combines real-time banking events with historical customer data to maintain a continuously updated customer view and support an AI-driven next-best action.

### Core message

> **What we know + what just happened -> what is true now -> what should we do next.**

The demo should focus on business value rather than infrastructure. The audience should see how the bank can recognize a meaningful change in customer behavior and respond before the customer moves more money elsewhere.

---

## 2. Customer Behavior Use Case

Use one fictional premium banking customer:

### Customer: Andi

Normal behavior:

- Receives a monthly salary
- Maintains an average balance of IDR 120 million
- Regularly uses the bank's credit card
- Makes mostly domestic transfers
- Frequently uses the mobile application
- Owns a mortgage and an investment account
- Normally transfers only IDR 5-10 million per month to external banks

### Simulated behavior change

Generate these events in sequence:

1. Salary deposit of IDR 35 million
2. External transfer of IDR 80 million
3. Mobile app search for "time deposit rate"
4. Additional external transfer of IDR 40 million
5. Optional event: declining credit-card activity

Individually, these events may not be meaningful. Together, they suggest that Andi may be moving funds to another bank.

---

## 3. Demo Story

### Initial customer state

```text
CUSTOMER: ANDI

Segment: Premium
Average balance: IDR 120M
Products owned: 4
Engagement: High
Churn risk: Low
```

### Event 1: Salary arrives

```text
10:01
Salary deposit: +IDR 35M
Churn risk: LOW
```

This is normal behavior and should not trigger an alert.

### Event 2: Large external transfer

```text
10:03
External transfer: -IDR 80M
Normal monthly external transfers: IDR 5-10M
Churn risk: LOW -> MEDIUM
```

The streamhouse compares the new transaction with Andi's normal behavior.

### Event 3: Product search

```text
10:05
Mobile app search: "Time Deposit Rate"
```

This event adds digital behavior context. It may indicate that Andi is comparing savings or investment options.

### Event 4: Second large external transfer

```text
10:07
External transfer: -IDR 40M
Total external transfers in 10 minutes: IDR 120M
Churn risk: MEDIUM -> HIGH
```

### Updated customer state

```text
CUSTOMER: ANDI

Current balance: IDR 42M
Normal average balance: IDR 120M
External fund movement: IDR 120M
Recent product search: Time deposit rate
Credit-card usage: Declining

Risk: HIGH
Reason: Significant behavioral change and possible fund migration
```

---

## 4. Simple Architecture

```text
Transaction Events       Digital Events
Transfers                Logins
Payments                 Product searches
Deposits                 Clickstream
       \                    /
        \                  /
         -> Event Stream <-
          Kafka/Confluent
                 |
                 v
        Stream Processing
          Apache Flink
                 |
                 | Join and enrich
                 v
       Historical Customer Data
       Profile, products, average
       balance, normal behavior
                 |
                 v
       Current Customer State
          "Andi right now"
                 |
                 v
          AI Assistant
                 |
                 v
        Next-Best Action
```

---

## 5. Minimum Demo Components

### A. Event generator

Use a small Python script to publish one event every few seconds.

Example sequence:

```text
+IDR 35M salary deposit
-IDR 80M external transfer
Search: time deposit rate
-IDR 40M external transfer
```

### B. Event streaming

Use one of the following:

- Apache Kafka for a local open-source setup
- Confluent Cloud for a managed demonstration

Suggested topics:

```text
bank.transactions
bank.digital-events
bank.customer-state
bank.alerts
```

### C. Stream processing

Use Apache Flink to:

- Read transaction and digital events
- Join events by `customer_id`
- Enrich events with customer profile data
- Calculate customer behavior indicators
- Update the current customer state
- Generate a high-risk alert when thresholds are met

### D. Historical customer data

Start with simple CSV files or database tables.

#### `customer.csv`

```text
customer_id,name,segment,avg_balance,normal_external_transfer,churn_risk
C001,Andi,Premium,120000000,10000000,LOW
```

#### `accounts.csv`

```text
customer_id,account_type,balance
C001,Savings,127000000
C001,Credit Card,0
C001,Mortgage,-850000000
C001,Investment,200000000
```

### E. Live customer dashboard

Display:

- Current balance
- Average balance
- External transfers in the last 24 hours
- Difference from normal behavior
- Recent digital activity
- Engagement level
- Current risk level
- Recommended action

A simple Streamlit application is sufficient for the first version.

### F. AI assistant

Provide the current customer state to an LLM using a controlled prompt.

Example prompt:

```text
You are a relationship manager assistant for a bank.

Analyze the customer's current behavior, historical behavior and
products. Identify significant changes and recommend the most
appropriate next action.

Do not contact the customer automatically. Explain the evidence
behind the recommendation. Do not make decisions involving credit,
pricing or regulatory reporting.
```

Expected response:

```text
A significant change in customer behavior has been detected.

Andi transferred IDR 120 million to an external bank, substantially
above his normal transfer behavior. His account balance has declined,
and he recently searched for time-deposit rates.

Recommended action:
Notify the assigned relationship manager to contact Andi, understand
his financial needs and discuss suitable deposit or investment options.
```

---

## 6. Example Processing Logic

Calculate these fields continuously:

```text
current_balance
external_transfer_10m
external_transfer_24h
external_transfer_vs_normal
recent_product_search
credit_card_activity_change
customer_risk
risk_reason
```

### Simple demo rules

```text
If external_transfer_10m > IDR 50M:
    risk = MEDIUM

If external_transfer_10m > IDR 100M
and recent_product_search == "time deposit rate":
    risk = HIGH

If a confirmed incoming transfer restores the balance:
    recalculate the risk
```

These thresholds are illustrative and must not be presented as production banking rules.

---

## 7. Five-Minute Demo Flow

### Minute 1: Explain the problem

> Banks have extensive customer data, but it is often spread across systems. By the time a behavior change is analyzed, the opportunity to respond may have passed.

### Minute 2: Show the normal customer state

Show Andi as a healthy premium customer with low risk.

### Minute 3: Start the event stream

Generate salary, transaction and digital behavior events one at a time.

### Minute 4: Show changing business context

Show the risk changing from low to medium and then high. Explain that the streamhouse is not simply moving messages. It is combining new events with historical customer behavior.

### Minute 5: Show AI-assisted action

Show the AI explanation and recommended relationship-manager action.

Conclude with:

> The streamhouse helps the bank understand what is true about the customer now. The AI assistant uses that trusted context to recommend what the bank should do next.

---

## 8. Build Plan

### Phase 1: Prepare data, 0.5-1 day

- Create one fictional customer profile
- Create account and normal-behavior data
- Define five demonstration events
- Define simple risk rules

### Phase 2: Build streaming pipeline, 1-2 days

- Configure Kafka or Confluent topics
- Build the Python event generator
- Build the Flink enrichment and scoring job
- Publish the updated customer state

### Phase 3: Build dashboard, 1 day

- Create a Streamlit interface
- Display incoming events
- Display the current customer state
- Visualize risk transitions

### Phase 4: Add AI recommendation, 0.5-1 day

- Create a controlled prompt
- Send only the enriched customer state to the model
- Display the explanation and recommended action
- Require human approval before any customer contact

### Phase 5: Rehearse and refine, 0.5 day

- Reset the demo state automatically
- Verify event timing
- Prepare a fallback recorded sequence
- Remove unnecessary technical details from the executive story

Estimated time for a focused proof of concept: **3-5 working days**.

---

## 9. What Not to Include in Version 1

Avoid combining too many use cases. Do not attempt to demonstrate all of these together:

- Fraud detection
- Anti-money laundering
- Credit scoring
- Customer churn
- Personalized marketing
- Customer service
- Cybersecurity

Keep the first demo focused on:

> **One customer + one behavior change + one updated customer state + one recommended action.**

---

## 10. Banking Safety and Governance

For the demonstration:

- Use only synthetic customer data
- Do not use real names, account numbers or transaction histories
- Keep the AI recommendation advisory
- Require a human relationship manager to approve any action
- Show the evidence behind each recommendation
- Log the events, rules and AI output
- Avoid automated credit, pricing, AML or adverse customer decisions
- Clearly label thresholds and risk scores as demo logic, not production policy

---

## 11. Success Criteria

The demo is successful if the audience can clearly see that:

1. New customer events arrive continuously.
2. Events are combined with historical customer information.
3. The current customer state changes immediately.
4. The system explains why the behavior is significant.
5. The AI recommends a reasonable human-led action.
6. The streamhouse complements rather than replaces historical data platforms.

---

## 12. Final Takeaway

> **A streamhouse turns disconnected live events into a continuously updated, trusted view of the customer. AI can then use that context to recommend a timely and relevant action.**
