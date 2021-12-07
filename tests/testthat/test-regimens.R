
test_that("createRegimens runs on Eunomia", {
  library(Eunomia)
  regimenIngredient <- data.frame(regimen_name = c("Venetoclax and Obinutuzumab", "Venetoclax and Obinutuzumab", "Doxycycline monotherapy"),
                                 regimen_id = c(35100084L, 35100084L, 35806103),
                                 ingredient_name = c("venetoclax", "obinutuzumab", "Doxycycline"),
                                 ingredient_concept_id = c(35604205L, 44507676L, 1738521))
  
  cd <- getEunomiaConnectionDetails()
  con <- connect(cd)
  createRegimens(con, regimenIngredient, "main", "main", "myregimens")
  
  # download the result from the database
  regimens <- dbGetQuery(con, "select * from myregimens")
  disconnect(con)
  expect_equal(nrow(regimens), 541)
})
