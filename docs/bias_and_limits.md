# Biais, limites et usages interdits

## Données synthétiques

Le jeu fourni est synthétique. Les résultats analytiques (taux de retour proches entre catégories,
répartition des segments) illustrent la méthode et ne doivent pas être lus comme des constats métier.

## Comparabilité des devises

Les montants sont en XOF, GHS et NGN. Les ordres de grandeur étant comparables dans les données,
la table de taux `currency_rates` est initialisée à 1,0 pour chaque devise. Conséquence : les
agrégats multi-pays additionnent des montants non convertis. Pour un usage réel, renseigner les taux
de change datés dans `dbt/seeds/currency_rates.csv`, ou analyser chaque pays séparément.

## Représentativité

- Les zones urbaines sont sur-représentées dans les commandes : les moyennes globales reflètent
  surtout les clients urbains.
- Les ventes par WhatsApp, appel téléphonique et agents (`WHATSAPP_BOT`, `CALL_CENTER`, `AGENT_BIZ`)
  ne sont pas des canaux numériques équivalents : leurs écarts de comportement ne doivent pas être
  attribués aux clients sans analyse préalable.
- La quarantaine retire des enregistrements (0,04 % des sous-totaux sont négatifs, 119 produits ont
  un prix nul, des identifiants clients sont ambigus). Les clients concernés sont absents de certaines
  analyses : les quantifier avant toute conclusion.

## Segmentation RFM : usages interdits

Le RFM sert au pilotage marketing (ciblage de campagnes, fidélisation). Il ne doit jamais servir à :

- refuser, restreindre ou dégrader un service, une livraison ou un support ;
- fixer un prix individuel ;
- exclure un client d'une offre pour un motif lié à sa localisation, son genre ou son âge.

Le score reflète le comportement d'achat passé, qui dépend lui-même du niveau de revenu et de
l'accès au numérique : l'utiliser pour exclure reviendrait à discriminer indirectement.

## Protection des données personnelles

- E-mails et téléphones sont re-hachés avec un sel secret (`HASH_SALT`, jamais commité).
  Un changement de sel change tous les hachages : le conserver de façon stable et protégée.
- Les noms sont supprimés dès la zone curated (minimisation).
- Le hachage ne rend pas les données anonymes au sens strict : l'accès aux tables `curated.customers`
  doit rester restreint.

## Limites techniques connues

- Spark tourne en mode local : adapté au volume du projet (quelques dizaines de milliers de lignes),
  pas à des téraoctets.
- La marge utilise le coût unitaire courant du catalogue, pas l'historique des coûts.
- Les dimensions SCD2 n'ont qu'une version par entité tant que la source ne change pas.
- Le DAG n'a qu'un exécuteur local : pas de haute disponibilité.
