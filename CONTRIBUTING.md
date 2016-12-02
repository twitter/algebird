---
layout: page
title:  "Contributing"
section: "contributing"
position: 5
---

# Contributing to Algebird

This page lists recommendations and requirements for how to best contribute to Algebird. We strive to obey these as best as possible. As always, thanks for contributing--we hope these guidelines make it easier and shed some light on our approach and processes.

## Key branches

- `master` is the latest, deployed version.
- `develop` is where development happens and all pull requests should be submitted.

## Pull requests

Submit pull requests against the `develop` branch. Try not to pollute your pull request with unintended changes. Keep it simple and small.

## Contributing Tests

All tests go into the test directory of the `algebird-test` subproject. This is because Algebird publishes a module of helpful functions and Scalacheck laws that anyone can use to test the algebraic properties of their own data structures.

We don't have strong conventions around our tests, but here are a few guidelines that will help both for contributing, and when trying to find tests for existing data structures.

### Scalacheck Properties

If you're adding [scalacheck](https://scalacheck.org/) properties, make a new class that extends `com.twitter.algebird.CheckProperties`. Here's an example of a new file of tests:

```scala
package com.twitter.algebird

import org.scalacheck.Prop

class MaxLaws extends CheckProperties {
  property("test description") {
    // Any instance of Prop will work.
    Prop.forAll { x: Int => x + 0 == x }
  }

  // Prop instances defined elsewhere work too:
  property("Max[Long] is a commutative monoid") {
    BaseProperties.commutativeMonoidLaws[Max[Long]]
  }
}
```

### Scalatest

We use [scalatest](http://www.scalatest.org/) for all of our other tests. [OperatorTest.scala](https://github.com/twitter/algebird/blob/1520068ae296d65ce3c4af7a0dc40137349f76f0/algebird-test/src/test/scala/com/twitter/algebird/OperatorTest.scala#L7) provides a nice example of this style of testing.

### What need testing the most?

The best way to figure out where we need tests is to look at our [Codecov coverage report](https://codecov.io/github/twitter/algebird). Codecov has an incredible [browser extension](http://docs.codecov.io/docs/browser-extension) that will embed coverage information right into the GitHub file browsing UI.

Once you've got the extension installed, navigate to the [algebird-core index](https://github.com/twitter/algebird/tree/develop/algebird-core/src/main/scala/com/twitter/algebird) to see file-by-file coverage percentages. Inside each file, lines that aren't covered by tests will be highlighted red.

## Contributing Documentation

The documentation for Algebird's website is stored in the `docs/src/main/tut` directory of the [docs subproject](https://github.com/twitter/algebird/tree/develop/docs).

Algebird's documentation is powered by [sbt-microsites](https://47deg.github.io/sbt-microsites/) and [tut](https://github.com/tpolecat/tut). `tut` compiles any code that appears in the documentation, ensuring that snippets and examples won't go out of date.

We would love your help making our documentation better. If you see a page that's empty or needs work, please send us a pull request making it better. If you contribute a new data structure to Algebird, please add a corresponding documentation page. To do this, you'll need to:

- Add a new Markdown file to `docs/src/main/tut/datatypes` with the following format:

```markdown
---
layout: docs
title:  "<Your Page Title>"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/<YourDataType>.scala"
scaladoc: "#com.twitter.algebird.<YourDataType>"
---

# Your Data Type

.....
```

- Make sure to add some code examples! Any code block of this form will get compiled using `tut`:


    ```toot:book
    <your code>
    ```

(Please replace `toot` with `tut`!) `tut` will evaluate your code as if you'd pasted it into a REPL and insert each line's results in the output. State persists across `tut` code blocks, so feel free to alternate code blocks with text discussion. See the [tut README](https://github.com/tpolecat/tut) for more information on the various options you can use to customize your code blocks.
- Add your page to the appropriate section in [the menu](https://github.com/twitter/algebird/tree/develop/docs/src/main/resources/microsite/data/menu.yml)

### Generating the Site

run `sbt docs/makeMicrosite` to generate a local copy of the microsite.

### Previewing the site

1. Install jekyll locally, depending on your platform, you might do this with any of the following commands:

```
yum install jekyll
apt-get install jekyll
gem install jekyll
```

2. In a shell, navigate to the generated site directory in `docs/target/site`
3. Start jekyll with `jekyll serve --incremental`
4. Navigate to http://127.0.0.1:4000/algebird/ in your browser
5. Make changes to your site, and run `sbt docs/makeMicrosite` to regenerate the site. The changes should be reflected as soon as `sbt docs/makeMicrosite` completes.

## Post-release

After the release occurs, you will need to update the documentation. Here is a list of the places that will definitely need to be updated:

 * `README.md`: update version numbers
 * `CHANGES.md`: summarize changes since last release

(Other changes may be necessary, especially for large releases.)

You can get a list of changes between release tags `v0.1.2` and `v0.2.0` via `git log v0.1.2..v0.2.0`. Scanning this list of commit messages is a good way to get a summary of what happened, although it does not account for conversations that occured on Github. (You can see the same view on the Github UI by navigating to <https://github.com/twitter/algebird/compare/v0.1.2...v0.2.0>.)

Once the relevant documentation changes have been committed, new [release notes](https://github.com/twitter/algebird/releases) should be added. You can add a release by clicking the "Draft a new release" button on that page, or if the relevant release already exists, you can click "Edit release".

The website should then be updated via `sbt docs/publishMicrosite`.

## License

By contributing your code, you agree to license your contribution under the terms of the [APLv2](LICENSE).
