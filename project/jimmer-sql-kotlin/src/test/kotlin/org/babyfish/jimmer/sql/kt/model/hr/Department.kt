package org.babyfish.jimmer.sql.kt.model.hr

import org.babyfish.jimmer.Formula
import org.babyfish.jimmer.jackson.JsonConverter
import org.babyfish.jimmer.jackson.LongToStringConverter
import org.babyfish.jimmer.sql.*
import testpkg.annotations.ConsiderAs
import testpkg.annotations.Module
import testpkg.annotations.Type
import java.time.LocalDateTime

@Entity
@Module("hr")
interface Department {

    @Id
    @get:ConsiderAs(types = [Type(String::class)])
    @JsonConverter(LongToStringConverter::class)
    val id: Long

    @JsonConverter(ConverterForIssue937::class)
    val name: String

    @LogicalDeleted("now")
    val deletedTime: LocalDateTime?

    @OneToMany(mappedBy = "department")
    val employees: List<Employee>

    @IdView("employees")
    val employeeIds: List<Long>

    @Formula(sql = "(select count(*) from employee where department_id = %alias.id)")
    val employeeCount: Long

    @Formula(dependencies = ["id", "name"])
    val description: String
        get() = "$id-${name.uppercase()}"
}