/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.admin.internal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class RedirectTest {

	private static final URI URI_1 = URI.create("http://localhost:8080");
	private static final URI URI_2 = URI.create("http://localhost:8088");

	private HttpServer server1, server2;
	private WebTarget shop1Target;

	@BeforeTest
	public void setup() {
		server1 = GrizzlyHttpServerFactory.createHttpServer(
				URI_1, ResourceConfig.forApplication(new ShopApp1()));
		server2 = GrizzlyHttpServerFactory.createHttpServer(
				URI_2, ResourceConfig.forApplication(new ShopApp2()));
		shop1Target = ClientBuilder.newClient().target(URI_1.toString() + "/books");
	}

	@AfterTest
	public void tearDown() {
		server1.shutdownNow();
		server2.shutdownNow();
	}

	static class BookResource extends BaseResource {
		protected BookResource(Client client, Authentication auth, long readTimeoutMs) {
			super(client, auth, readTimeoutMs);
		}
	}

	@Test
	public void testGetRedirect() throws Exception {
		BookResource resource = new BookResource(ClientBuilder.newClient(), new AuthenticationDisabled(), 20);
		resource.asyncGetRequest(shop1Target.path("1"), new InvocationCallback<Book>() {
			@Override
			public void completed(Book b) {
				assertEquals(b.getName(), "Awesome");
			}

			@Override
			public void failed(Throwable throwable) {
				fail(throwable.getMessage(), throwable);
			}
		}).get();

		resource.asyncGetRequest(shop1Target.path("2"), new InvocationCallback<Book>() {
			@Override
			public void completed(Book b) {
				fail("book with id 2 shouldn't exists in bookshop2");
			}

			@Override
			public void failed(Throwable throwable) {
				Throwable cause = throwable.getCause();
				assertTrue(cause instanceof ClientErrorException);
				assertEquals(((ClientErrorException) cause).getResponse().getStatus(), 404);
			}
		});

		Thread.sleep(10000);
	}

	@Test
	public void testPutRedirect() throws Exception {

	}

	@Test
	public void testPostRedirect() throws Exception {

	}

	@Test
	public void testDeleteRedirect() throws Exception {

	}

	@Path("books")
	public static class BookShop1 {
		@GET
		@Path("{id}")
		@Produces(MediaType.APPLICATION_JSON)
		public Response getBook(@PathParam("id") int id) {
			String a = URI_2.toString() + "/books/" + id;
//			throw new WebApplicationException(Response.temporaryRedirect(URI.create(a)).build());
			return Response.temporaryRedirect(URI.create(a)).build();
		}

		@POST
		@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
		@Produces(MediaType.APPLICATION_JSON)
		public Response createBook(@FormParam("name") String name) {
			return Response.temporaryRedirect(URI_2).build();
		}

		@PUT
		@Path("{id}")
		@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
		@Produces(MediaType.APPLICATION_JSON)
		public Response updateOrCreateBook(@PathParam("id") int id, @FormParam("name") String name) {
			return Response.temporaryRedirect(URI_2).build();
		}

		@DELETE
		@Path("{id}")
		@Produces(MediaType.APPLICATION_JSON)
		public Response deleteBook(@PathParam("id") int id) {
			return Response.temporaryRedirect(URI_2).build();
		}
	}

	@Path("books")
	public static class BookShop2 {

		static final AtomicInteger id = new AtomicInteger(1);
		static final Map<Integer, Book> books =
				new HashMap<>(Collections.singletonMap(id.get(), new Book(id.get(), "Awesome")));

		@GET
		@Path("{id}")
		@Produces(MediaType.APPLICATION_JSON)
		public Response getBook(@PathParam("id") int id) {
			if (books.containsKey(id)) {
				return Response.ok(books.get(id).toJson()).build();
			} else {
				return Response.status(Response.Status.NOT_FOUND).build();
			}
		}

		@POST
		@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
		@Produces(MediaType.APPLICATION_JSON)
		public Response createBook(@FormParam("name") String name) {
			Book book = new Book(id.incrementAndGet(), name);
			books.put(book.id, book);
			URI uri = UriBuilder.fromUri(URI_2).path("books").path("" + book.id).build();
			return Response.created(uri).build();
		}

		@PUT
		@Path("{id}")
		@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
		@Produces(MediaType.APPLICATION_JSON)
		public Response updateOrCreateBook(@PathParam("id") int id, @FormParam("name") String name) {
			Book book = new Book(id, name);
			books.put(book.id, book);
			return Response.ok().entity(book.toJson()).build();
		}

		@DELETE
		@Path("{id}")
		@Produces(MediaType.APPLICATION_JSON)
		public Response deleteBook(@PathParam("id") int id) {
			if (books.containsKey(id)) {
				Book book = books.remove(id);
				return Response.ok().entity(book.toJson()).build();
			} else {
				return Response.status(Response.Status.NOT_FOUND).build();
			}
		}
	}

	public static class ShopApp1 extends Application {

		@Override
		public Set<Class<?>> getClasses() {
			Set<Class<?>> set = new HashSet<>();
			set.add(BookShop1.class);
			return set;
		}

		@Override
		public Set<Object> getSingletons() {
			return Collections.emptySet();
		}
	}

	public static class ShopApp2 extends Application {

		@Override
		public Set<Class<?>> getClasses() {
			Set<Class<?>> set = new HashSet<>();
			set.add(BookShop2.class);
			return set;
		}

		@Override
		public Set<Object> getSingletons() {
			return Collections.emptySet();
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Book {
		int id;
		String name;

		public String toJson() {
			return String.format("{\"id\":%d,\"name\":\"%s\"}", id, name);
		}
	}

}
