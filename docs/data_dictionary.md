# Dictionnaire de données

Volumes de référence (jeu fourni) : 50 000 commandes, 81 180 clients, 12 000 produits,
46 141 paiements, 105 409 lignes de commande. Les montants sont exprimés dans la devise de la
commande (XOF, GHS ou NGN).

## Zone curated (PostgreSQL, schéma `curated`)

| Table | Clé | Contenu |
|---|---|---|
| `orders` | `order_id` | Commandes retenues, avec `captured_amount`, `has_payment`, `is_partial_payment`, `customer_dq_flag`, partition `order_year_month` |
| `order_lines` | `order_line_id` | Lignes de commande validées (produit connu, commande retenue) |
| `customers` | `customer_id` | Clients sans nom, `email_hash` et `phone_hash` salés (SHA-256, 64 caractères), `master_customer_id`, `is_duplicate_account` |
| `products` | `product_id` | Catalogue typé, `dq_flag` (`ZERO_PRICE`, `COST_GT_PRICE`) |
| `payments` | `payment_id` | Paiements, `is_selected` = paiement capturé retenu pour la commande |
| `deliveries` | `delivery_id` | Livraisons avec `delivery_delay_days`, `is_late` (7 jours ou plus), `status_conflict` |
| `quarantine` | `source`, `record_key`, `reason` | Enregistrements rejetés et motif, contenu d'origine en JSON (`payload`) |

## Zone analytics (schéma `analytics`)

### Schéma en étoile

| Table | Grain | Colonnes principales |
|---|---|---|
| `fact_order_line` | une ligne de commande | clés `customer_key`, `product_key`, `order_date_key`, `payment_method_key`, `delivery_status_key` ; mesures `quantity`, `unit_price`, `line_discount`, `line_total`, `line_cost`, `line_margin` ; indicateurs `is_late`, `is_partial_payment` |
| `dim_customer` | une version d'un client | `customer_key`, `customer_id`, `master_customer_id`, segment, fidélité, `valid_from`, `valid_to`, `is_current` ; membre inconnu `-1` |
| `dim_product` | une version d'un produit | `product_key`, `product_id`, catégorie, marque, `unit_cost`, `unit_price`, `valid_from`, `valid_to`, `is_current` |
| `dim_date` | un jour (2024-2027) | `date_key` (AAAAMMJJ), année, trimestre, mois, `is_weekend` |
| `dim_payment_method` | un mode de paiement | `payment_method`, `payment_group` ; valeur `NONE` si aucun paiement |
| `dim_delivery_status` | un statut | `delivery_status`, `is_final`, `is_successful` ; valeur `NO_DELIVERY` |

Note : `line_cost` utilise le coût unitaire courant du catalogue (pas l'historique de coût).

### Marts métier

| Table | Contenu | Règle |
|---|---|---|
| `mart_customer_rfm` | un client maître | Récence, fréquence, montant, scores de 1 à 5 par quintiles, segment (`CHAMPIONS`, `LOYAL`, `NEW_OR_PROMISING`, `AT_RISK`, `LOST`, `NEED_ATTENTION`). Commandes `CANCELLED`, `RETURNED`, `PENDING` exclues. Date de référence : variable dbt `as_of_date` |
| `mart_product_rotation` | un produit actif | Unités, chiffre d'affaires, marge, classe ABC (80 % / 95 % du CA cumulé), statut `ACTIVE`, `SLOW` ou `DORMANT` |
| `mart_category_return_rate` | mois x pays x catégorie | Taux de retour en pourcentage, modèle incrémental (deux derniers mois recalculés) |

## Table d'exploitation

`ops.pipeline_runs` : une ligne par exécution du DAG (`run_id`, `state`, `duration_s`,
`failed_tasks`, `orders_count`, `quarantine_count`).

## Règles de qualité appliquées au curated

| Motif de quarantaine | Règle |
|---|---|
| `NO_LINES` | commande sans ligne de commande valide |
| `BAD_TOTAL` | total différent de sous-total + livraison - remise (tolérance 0,05) |
| `ORPHAN_PRODUCT` | ligne dont le produit n'existe pas |
| `ORDER_NOT_IN_CLEAN` | ligne ou livraison dont la commande n'est pas retenue |
| `AMBIGUOUS_CUSTOMER_ID` | même `customer_id` partagé par des personnes différentes |
