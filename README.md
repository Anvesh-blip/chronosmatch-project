# 🚀 ChronosMatch - High-Performance Matching Engine

Welcome to the **ChronosMatch** project repository! This repository houses an ultra-low latency, high-performance Limit Order Book (LOB) matching engine engineered for lightning-fast financial transaction processing and deterministic trade execution.

---

## 🎯 Developer Profile & Role
* **Engineer Name:** Prankur Shukla
* **Assigned Branch:** `Prankur-dev`
* **Core Responsibilities:** 
  * Designing and implementing the core Limit Order Book data structures.
  * Developing deterministic price-time priority matching logic (BUY/SELL side).
  * Ensuring modular, clean, and maintainable code architecture ready for system integration.

---

## ⚙️ Technical Specifications & Architecture

* **Pricing Engine (`price_ticks`):** 
  * Operates entirely on integer-based `price_ticks` (calibrated at 100 ticks per rupee). 
  * Avoids floating-point calculation inaccuracies, ensuring maximum execution speed and numerical precision.
* **Binary Packet Layout (`!IIII`):** 
  * Compliant with a rigid 16-byte binary packet structure designed for low-overhead network transmission and parsing.
  * Handles packed fields: `order_id`, `price_ticks`, `quantity`, and `side`.
* **Matching Algorithm:** 
  * Implements strict price-time priority rules.
  * Fully supports full fills, partial fills, and real-time trade event generation.

---

## 📂 Repository Structure

```text
chronosmatch-project/
│
├── src/
│   └── engine/
│       ├── __init__.py        # Package initializer
│       └── order_book.py      # Core Limit Order Book & Matching Engine logic
│
├── README.md                  # Project documentation & architecture guide
└── app.py                     # Application entry point and utilities
