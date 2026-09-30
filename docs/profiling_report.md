# Rapport de profilage (phase 2)

## Vue d'ensemble

| fichier | lignes | colonnes | valeurs manquantes |
| --- | --- | --- | --- |
| orders | 52000 | 18 | promo_code=32220 |
| order_lines | 107517 | 9 | aucune |
| customers | 81200 | 14 | aucune |
| products | 12000 | 15 | aucune |
| payments | 46141 | 9 | aucune |
| deliveries | 41218 | 9 | delivered_at=11651 |

## Perimetre

Commandes du 2025-05-01 au 2026-04-30, pays : CIV, GHA, NGA, SEN.

## Catalogue des anomalies

| anomalie | nombre | part | traitement prevu |
| --- | --- | --- | --- |
| orders : order_id en doublon | 1500 | 2.88 % | Garder la version la plus recente (updated_at) |
| orders : total_amount != sous-total + livraison - remise | 500 | 0.99 % | Quarantaine |
| orders : aucune ligne dans order_lines | 500 | 0.99 % | Quarantaine (recalcul impossible) |
| orders : sous-total != somme des lignes | 2070 | 4.14 % | Signaler, recalculer depuis les lignes |
| orders : client inconnu | 0 | 0.00 % | Quarantaine |
| order_lines : product_id inexistant | 2108 | 1.96 % | Quarantaine |
| order_lines : line_total != quantite x prix - remise | 2108 | 1.96 % | Recalculer / signaler |
| order_lines : order_id inexistant | 0 | 0.00 % | Quarantaine |
| customers : email_hash en doublon | 1200 | 1.48 % | Client maitre (MDM) : regrouper les comptes |
| customers : phone_hash en doublon | 1200 | 1.48 % | Client maitre (MDM) |
| customers : customer_id en doublon (personnes differentes) | 10 | 0.01 % | Cle de substitution + quarantaine a arbitrer |
| products : unit_price = 0 | 119 | 0.99 % | Quarantaine |
| products : unit_cost > unit_price (prix > 0) | 60 | 0.50 % | Quarantaine |
| payments : commandes avec plusieurs paiements | 390 | 0.77 % | Retenir le paiement CAPTURED, ignorer les FAILED |
| payments : commandes avec plusieurs CAPTURED | 98 | 0.27 % | Dedoublonner : un seul paiement capture par commande |
| payments : paiement partiel (capture < total) | 320 | 0.88 % | Flag paiement_partiel, conserver |
| payments : transaction_reference en doublon | 9 | 0.02 % | Signaler |
| orders : DELIVERED sans aucun paiement | 1093 | 2.16 % | Signaler |
| deliveries : livraison >= 7 jours apres la commande | 1451 | 4.91 % | Arrivee tardive : reconcilier par order_id |
| deliveries : DELIVERED alors que la commande n'est pas DELIVERED | 6200 | 15.04 % | Statut de livraison fait foi, signaler |
