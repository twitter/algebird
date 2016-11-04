---
layout: docs
title:  "Data Types"
section: "data"
position: 2
---

{% include_relative datatypes/datatypes.md %}

### Index

{% for x in site.pages %}
  {% if x.section == 'data' %}
- [{{x.title}}]({{site.baseurl}}{{x.url}})
  {% endif %}
{% endfor %}
