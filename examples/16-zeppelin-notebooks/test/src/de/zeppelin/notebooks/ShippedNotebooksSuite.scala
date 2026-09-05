package de.zeppelin.notebooks

/**
 * The test that earns its keep.
 *
 * A Zeppelin note is data, not code, so nothing else in the build would notice a typo in a magic, a renamed interpreter
 * setting or a file name that no longer matches the note inside it. These assertions read the exact files the Docker
 * Compose stack installs in the container, and they need no container to do it.
 */
final class ShippedNotebooksSuite extends munit.FunSuite {

  private val config = InterpreterConfig
    .parse(Resources.text(NotebookLibrary.interpreterConfigPath))
    .fold(failure => fail(s"the shipped interpreter configuration is unusable: $failure"), identity)

  private val notebooks: List[(String, Notebook)] =
    NotebookLibrary.fileNames.map { fileName =>
      fileName -> NotebookLibrary
        .notebook(fileName)
        .fold(failure => fail(s"notebook '$fileName' is unusable: $failure"), identity)
    }

  test("the stack configures exactly the interpreters the notebooks advertise") {
    assertEquals(config.names, Set("spark", "md", "jdbc"))
  }

  NotebookLibrary.fileNames.foreach { fileName =>
    test(s"'$fileName' is well-formed and only uses configured interpreters") {
      val note = notebooks.toMap.apply(fileName)
      assertEquals(NotebookCheck.problems(fileName, note, config), Nil)
    }
  }

  test("every note explains itself in a Markdown paragraph") {
    notebooks.foreach { case (fileName, note) =>
      assert(note.paragraphs.exists(_.magic.contains(Magic("md", None))), s"$fileName has no %md paragraph")
    }
  }

  test("the lakehouse tour reads Delta Lake with Spark and queries it with Spark SQL") {
    val (_, tour) = notebooks.head
    assert(tour.paragraphs.exists(_.magic.contains(Magic("spark", None))), "no Scala paragraph")
    assert(tour.paragraphs.exists(_.magic.contains(Magic("spark", Some("sql")))), "no %spark.sql paragraph")
    assert(tour.paragraphs.exists(_.text.contains("""format("delta")""")), "no Delta Lake read")
  }

  test("the federation note queries Trino through the jdbc interpreter") {
    val (_, federation) = notebooks(1)
    assert(federation.paragraphs.exists(_.magic.contains(Magic("jdbc", None))), "no %jdbc paragraph")
    assertEquals(config.setting("jdbc").flatMap(_.properties.get("default.url")), Some("jdbc:trino://trino:8080"))
    assertEquals(
      config.setting("jdbc").flatMap(_.properties.get("default.driver")),
      Some("io.trino.jdbc.TrinoDriver")
    )
  }

  test("a country dropdown lets a reader change the analysis without editing SQL") {
    val dropdowns = notebooks.flatMap(_._2.paragraphs).flatMap(_.forms).filter(_.isDropdown)
    assert(dropdowns.nonEmpty, "no dynamic form with choices is shipped")
    dropdowns.foreach { form =>
      assertEquals(form.options.sorted, List("DE", "ES", "FR", "PL", "UA"))
      assert(form.defaultValue.exists(form.options.contains), s"${form.name} defaults outside its own choices")
    }
  }

  test("the Spark interpreter is pointed at the object store and at Delta Lake") {
    val spark = config.setting("spark").getOrElse(fail("no spark interpreter setting"))
    assertEquals(spark.properties.get("spark.hadoop.fs.s3a.endpoint"), Some("http://minio:9000"))
    assertEquals(spark.properties.get("spark.sql.extensions"), Some("io.delta.sql.DeltaSparkSessionExtension"))
    assertEquals(spark.properties.get("SPARK_HOME"), Some("/opt/lakehouse/spark"))
  }
}
