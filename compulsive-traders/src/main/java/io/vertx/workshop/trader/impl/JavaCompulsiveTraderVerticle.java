package io.vertx.workshop.trader.impl;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.types.EventBusService;
import io.vertx.servicediscovery.types.MessageSource;
import io.vertx.workshop.common.MicroServiceVerticle;
import io.vertx.workshop.portfolio.PortfolioService;

/**
 * A compulsive trader...
 */
public class JavaCompulsiveTraderVerticle extends MicroServiceVerticle {

	@Override
	public void start(Future<Void> future) {
		super.start();

		// DONE
		// ----
		String company = TraderUtils.pickACompany();
		int numberOfShares = TraderUtils.pickANumber();
		System.out.println(this.getClass().getSimpleName() + " configured for company " + company + " and shares "
				+ numberOfShares);

		Future<MessageConsumer<JsonObject>> marketFuture = Future.future();
		Future<PortfolioService> portfolioFuture = Future.future();

		// DONE does this also work with vertx.eventBus().consumer()? (see
		// RestQuoteAPIVertical) -> no because this consummation discovers the service
		// whereas vertex.eventBus().consumer works on the given vertex which is or hold
		// the service.
		
		// Discover the market-data MessageSource published by GeneratorConfigVerticle,
		// sending quotes of all configured companies.
		// Future will hold a MessageConsumer of a JsonObject which is actually the
		// quote.
		// The completer simply puts the result in the future on success of set it to
		// failed otherwise
		MessageSource.getConsumer(discovery, new JsonObject().put("name", "market-data"), marketFuture.completer());

		// Discover the PortfolioSevice proxy to perform async RPC
		EventBusService.getProxy(discovery, PortfolioService.class, portfolioFuture.completer());

		CompositeFuture.all(marketFuture, portfolioFuture).setHandler(ar -> {
			// checks both futures. Only valid if both futures are okay
			if (ar.failed()) {
				future.fail(ar.cause());
			} else {
				MessageConsumer<JsonObject> marketConsumer = marketFuture.result();
				PortfolioService portfolioService = portfolioFuture.result();

				// handle the massageConsumer of the marketFuture
				// the handler is called every time a quote is received and starts the trading
				// logic
				marketConsumer.handler(message -> {
					JsonObject quote = message.body();
					TraderUtils.dumbTradingLogic(company, numberOfShares, portfolioService, quote);
				});

				future.complete();
			}
		});
		// ----
	}

}
