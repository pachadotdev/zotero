# zotero

[![BuyMeACoffee](https://raw.githubusercontent.com/pachadotdev/buymeacoffee-badges/main/bmc-yellow.svg)](https://www.buymeacoffee.com/pacha)

A simple package to import Zotero collections into R and export those as tidy BIB files.

Install the zotero R package with:

```r
remotes::install_github("pachadotdev/zotero")
```

Read Zotero files like so:

```r
library(zotero)
x = read_zotero("bibliography.bib")
y = read_zotero("bibliography.rdf")
z = read_zotero("bibliography.ris")
```

The result is the same for each object:

```
> names(z)
[1] "df"  "bib"
> z
$df
                                                                                                       title
1                                                    Buy Stata | Student single-user purchases (educational)
2                                                        Buy Stata | Student Lab new purchases (educational)
3                                                                 A Deep Dive Into How R Fits a Linear Model
...
                                                                   author year
1                                                              Stata Corp 2025
2                                                              Stata Corp 2025
3                                                           Matthew Drury 2016
...

$bib
Corp S (2025). “Buy Stata | Student single-user purchases
(educational).” Stata Student single-user purchases,
<https://www.stata.com/order/new/edu/profplus/student-pricing/>.

Corp S (2025). “Buy Stata | Student Lab new purchases (educational).”
Educational single-user new purchases,
<https://www.stata.com/order/new/edu/lab-licenses/dl/>.

Drury M (2016). “A Deep Dive Into How R Fits a Linear Model.”
<https://madrury.github.io/jekyll/update/statistics/2016/07/20/lm-in-R.html>.
...
```

Export the result to a Clarivate-BIB file:

```r
write_clarivate(z$bib, "export.bib")
```
