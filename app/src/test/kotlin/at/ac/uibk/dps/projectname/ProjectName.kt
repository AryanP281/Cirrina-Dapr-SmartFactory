package at.ac.uibk.dps.projectname

import at.ac.uibk.dps.projectname.generated.BuildInfo
import java.io.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ProjectName {
  @Test
  fun `the version of the project should match the version in version txt`() {
    assertEquals(File("version.txt").readText(), BuildInfo.VERSION)
  }
}
