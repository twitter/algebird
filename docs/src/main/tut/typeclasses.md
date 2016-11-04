---
layout: docs
title:  "Abstract Algebra"
section: "typeclasses"
position: 1
---
{% include_relative typeclasses/abstract_algebra.md %}

## Index

{% for x in site.pages %}
{% if x.section == 'typeclasses' %}
- [{{x.title}}]({{site.baseurl}}{{x.url}})
{% endif %}
{% endfor %}
