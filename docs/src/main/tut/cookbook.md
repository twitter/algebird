---
layout: docs
title:  "Cookbook"
section: "cookbook"
position: 6
---

{% include_relative cookbook/cookbook.md %}

{% for x in site.pages %}
  {% if x.section == 'cookbook' %}
- [{{x.title}}]({{site.baseurl}}{{x.url}})
  {% endif %}
{% endfor %}
