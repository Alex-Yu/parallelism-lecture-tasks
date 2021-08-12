package edu.alexy.lectures.concurrency

import edu.alexy.lectures.concurrency.model.Company
import edu.alexy.lectures.concurrency.util.{Db, DbError, WebApi, WebApiError}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object RoboAdviser {
  // Task 1.
  // Return 'AAPL' revenue from `Db.getCompanyLastFinancials`. Possible error should be returned as a ServiceError.
  def getAAPLRevenue: Future[Double] =
    Db.getCompanyLastFinancials("AAPL")
      .transformWith {
        case Success(Some(financials)) => Future.successful(financials.revenue)
        case _                         => Future.failed(DbError)
      }

  private def futureRetry[T](block: => Future[T], retries: Int): Future[T] = {
    val result: Future[T] = block

    result.recoverWith {
      case _ if retries > 1 => futureRetry[T](block, retries - 1)
    }
  }

  // Task 2.
  // Implement a fallback strategy for 'Db.getAllTickers'.
  // 'Db.getAllTickers' should be called no more then 'retries' times.
  // Possible error should be returned as a ServiceError.
  def getAllTickersRetryable(retries: Int = 10): Future[Seq[String]] =
    futureRetry(Db.getAllTickers, retries).transform {
      case Failure(exception) => Failure(DbError)
      case other              => other
    }

  // Task 3.
  // Implement a fallback strategy for 'Db.getCompanyLastFinancials'.
  // 'Db.getCompanyLastFinancials' should be called no more then 'retries' times.
  // Possible error should be returned as a ServiceError.
  def getCompanyRetryable(ticker: String,
                          retries: Int = 10): Future[Option[Company]] =
    futureRetry(Db.getCompanyLastFinancials(ticker), retries).transform {
      case Failure(exception) => Failure(DbError)
      case other              => other
    }

  // Task 4.
  // Implement a fallback strategy 'WebApi.getPrice'.
  // 'WebApi.getPrice' should be called no more then 'retries' times.
  // Possible error should be returned as a ServiceError.
  def getPriceRetryable(ticker: String, retries: Int = 10): Future[Double] =
    futureRetry(WebApi.getPrice(ticker), retries).transform {
      case Failure(exception) => Failure(WebApiError)
      case other              => other
    }

  // Task 5.
  // Using retryable functions return all tickers with their real time prices.
  def getAllTickersPrices: Future[Seq[(String, Double)]] =
    for (tickers <- getAllTickersRetryable();
         tikerWithPrice <- Future.traverse(tickers) { ticker =>
           Future.successful(ticker).zip(getPriceRetryable(ticker))
         }) yield tikerWithPrice

  // Task 6.
  // Using `getCompanyRetryable` and `getPriceRetryable` functions return a company with its real time stock price.
  def getCompanyFinancialsWithPrice(ticker: String): Future[(Company, Double)] = {
    getCompanyRetryable(ticker).collect {
      case Some(company) =>
        Future.successful(company).zip(getPriceRetryable(company.ticker))
    }
  }.flatten

  // Task 7.
  // Implement a function that returns a list of chip ('Company.isCheap') companies
  // with their real time stock prices using 'getAllTickersRetryable' and
  // 'getCompanyFinancialsWithPrice' functions.
  def buyList: Future[Seq[(Company, Double)]] = {
    for (tickers <- getAllTickersRetryable();
         companyWithPriceSeq <- Future.traverse(tickers) { ticker =>
           getCompanyFinancialsWithPrice(ticker)
             .collect {
               case companyWithPrice @ (company, price)
                   if company.isCheap(price) =>
                 Success(companyWithPrice)
             }
             .recover { case x => Failure(x) }
         }) yield {
      companyWithPriceSeq.collect {
        case Success(companyWithPrice) => companyWithPrice
      }
    }
  }

  // Task 8.
  // Implement a function that returns a list of expensive ('Company.isExpensive') companies
  // with their real time stock prices using 'getAllTickersRetryable', 'getCompanyRetryable',
  // 'getPriceRetryable' and zipping.
  def sellList: Future[Seq[(Company, Double)]] = {
    for (tickers <- getAllTickersRetryable();
         companyWithPriceSeq <- Future.traverse(tickers) { ticker =>
           getCompanyRetryable(ticker)
             .zip(getPriceRetryable(ticker))
             .collect {
               case (Some(company), price) if company.isExpensive(price) =>
                 Success((company, price))
             }
             .recover { case x => Failure(x) }
         }) yield {
      companyWithPriceSeq.collect {
        case Success(companyWithPrice) => companyWithPrice
      }
    }
  }
}
