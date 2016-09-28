package com.github.tminglei.slickpg.lateral

import slick.ast._
import slick.compiler.CompilerState
import slick.driver.{JdbcDriver, PostgresDriver}
import slick.lifted.WrappingQuery

trait LateralSupport extends JdbcDriver with PostgresDriver { driver =>
  import driver.api._

  case object LateralJoin extends JoinType("lateral")

  override def createQueryBuilder(n: Node, state: CompilerState): QueryBuilder = new QueryBuilder(n, state)

  class QueryBuilder(tree: Node, state: CompilerState) extends super.QueryBuilder(tree, state) {
    import slick.util.MacroSupport.macroSupportInterpolation
    override protected def buildJoin(j: Join): Unit = {
      j.jt match {
        case LateralJoin =>
          buildFrom(j.left, Some(j.leftGen))
          b"\nlateral "
          buildFrom(j.right, Some(j.rightGen))
        case _ => super.buildJoin(j)
      }
    }
  }

  implicit class QueryExtension[E, U, C[_]](q: Query[E, U, C]) {
    def lateral[E2, U2, D[_]](f: E => Query[E2, U2, D]) = {
      val leftGen, rightGen = new AnonSymbol
      val aliased1 = q.shaped.encodeRef(Ref(leftGen))
      val q2 = f(aliased1.value)
      val aliased2 = q2.shaped.encodeRef(Ref(rightGen))
      val shape = aliased1.zip(aliased2)
      new WrappingQuery[(E, E2), (U, U2), C](
        Join(leftGen, rightGen, q.toNode, q2.toNode, LateralJoin, LiteralNode(true)), shape
      )
    }
  }
}
